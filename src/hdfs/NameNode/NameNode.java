package hdfs.NameNode;

import global.Hdfs;
import hdfs.DataNode.DataNodeRemoteInterface;

import hdfs.IO.HDFSInputStream;

import hdfs.DataStructure.DataNodeEntry;
import hdfs.DataStructure.HDFSChunk;
import hdfs.DataStructure.HDFSFile;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NameNode implements NameNodeRemoteInterface{
	volatile long chunkNaming;
	private Map<String, DataNodeAbstract> dataNodeTbl;
	private Map<String, HDFSFile> fileTbl;
	private Map<String, DataNodeRemoteInterface> dataNodeStubTbl;
	private Queue<DataNodeAbstract> selector = new ConcurrentLinkedQueue<DataNodeAbstract>();
	private NameNodeRemoteInterface nameNodeStub;
	private String ip;
	private int port;
	
	public NameNode(int port) {
		this.dataNodeTbl = new ConcurrentHashMap<String, DataNodeAbstract>();
		this.fileTbl = new ConcurrentHashMap<String, HDFSFile>();
		this.dataNodeStubTbl = new ConcurrentHashMap<String, DataNodeRemoteInterface>();
		this.chunkNaming = 1;
		this.port = port;

	}
	
	/* Export and bind NameNode remote object */
	public void init() {
		try {
			this.ip = Inet4Address.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			System.err.println("Err: Name node is not accessible to network. Now shut down the system");
			System.exit(-1);
		}
		
		/* Export and bind RMI */
		Registry registry = null;
		try {
			registry = LocateRegistry.createRegistry(this.port);
			this.nameNodeStub = (NameNodeRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
			registry.rebind("NameNode", nameNodeStub);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		Thread systemCheckThread = new Thread(new SystemCheck());
		systemCheckThread.start();
	}
	
	@Override
	public synchronized void heartBeat(String dataNodeName) {
		DataNodeAbstract dataNodeInfo = dataNodeTbl.get(dataNodeName);
		if (dataNodeInfo != null) {
			dataNodeInfo.latestHeartBeat = new Date();
		}
	}
	
	public String join(String ip, int port, List<String> chunkNameList) throws RemoteException {
		String dataNodeName = ip + ":" + port;
		DataNodeAbstract dataNodeInfo = new DataNodeAbstract(ip, port, dataNodeName);
		Registry dataNodeRegistry = LocateRegistry.getRegistry(ip, port);
		try {
			DataNodeRemoteInterface dataNodeStub= (DataNodeRemoteInterface)dataNodeRegistry.lookup("DataNode");
			this.dataNodeStubTbl.put(dataNodeName, dataNodeStub);
			dataNodeTbl.put(dataNodeName, dataNodeInfo);
			selector.offer(dataNodeInfo);
			if (Hdfs.DEBUG) {
				System.out.format("DEBUG NameNode.join(): %s joins cluster.\n", dataNodeName);
			}
			return dataNodeName;
		} catch (NotBoundException e) {
			return null;
		}
	}

	@Override
	public void chunkReport(String dataNodeName, List<String> chunkList) throws RemoteException {
		DataNodeAbstract dataNodeInfo = this.dataNodeTbl.get(dataNodeName);
		dataNodeInfo.chunkList = chunkList;
	}
	
	
	@Override
	public HDFSFile create(String filePath) throws RemoteException {
		if (this.fileTbl.containsKey(filePath)) {
			return null;
		} 
		HDFSFile newFile = new HDFSFile(filePath, Hdfs.replicaFactor, this.nameNodeStub);
		this.fileTbl.put(filePath, newFile);
		return newFile;
	}
	
	/**
	 * Open a file by return a HDFSInputStream with all the chunks' info
	 * associate with that file
	 * 
	 * @param fileName path of the file to be opened
	 * @return HDFSInputStream 
	 * @throws RemoteException
	 */
	public HDFSFile open(String fileName) throws RemoteException {
		HDFSFile file = this.fileTbl.get(fileName);
//		HDFSInputStream in = null;
//		if (file != null) {
//			List<ChunkInfo> chunkInfoList = file.chunkList;
//			String hostIP = null;
//			try {
//				hostIP = Inet4Address.getLocalHost().getHostName();
//			} catch (UnknownHostException e) {
//				e.printStackTrace();
//				throw new RemoteException("Unknown Host");
//			}
//			
//		}
		
		return file;
	}
	
		
	@Override
	public void delete(String path) throws RemoteException, IOException {
		HDFSFile file = this.fileTbl.get(path);
		if (file == null) {
			return;
		}
		for (HDFSChunk chunk : file.getChunkList()) {
			for (DataNodeEntry dataNode : chunk.getAllLocations()) {
				try {
					Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNode.dataNodeRegistryIP, dataNode.dataNodeRegistryPort);
					DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) dataNodeRegistry.lookup("DataNode");
					dataNodeStub.deleteChunk(chunk.getChunkName());
				} catch (RemoteException e) {
					throw new IOException("Cannot connect to DataNode");
				} catch (NotBoundException e) {
					throw new IOException("Cannot connect to DataNode");
				} catch (IOException e) {
					throw new IOException("Cannot delete chunk(name:" + chunk.getChunkName() + ")");
				}
			}
		}
		this.fileTbl.remove(path);
	}
	
	
	public synchronized String nameChunk() {
		String chunkName = String.format("%010d", this.chunkNaming++);
		return chunkName;
	}
	
	public synchronized List<DataNodeEntry> select(int replicaFactor) {
		List<DataNodeEntry> rst = new ArrayList<DataNodeEntry>();
		int counter = 0;
		while (counter < replicaFactor && !this.selector.isEmpty()) {
			DataNodeAbstract chosenDataNode = this.selector.poll();
			rst.add(new DataNodeEntry(chosenDataNode.dataNodeRegistryIP, chosenDataNode.dataNodeRegistryPort, chosenDataNode.dataNodeName));
			counter++;
			this.selector.offer(chosenDataNode);
		}
		
		if (Hdfs.DEBUG) {
			ArrayList<String> list = new ArrayList<String>();
			
			for (DataNodeEntry dn : rst) {
				list.add(dn.getNodeName());
			}
			String print_rst = String.format("DEBUG NameNode.select(): %s", list.toString());
			System.out.println(print_rst);
		}
		
		return rst;
	}


	public synchronized void commitFile(HDFSFile file) throws RemoteException {
		this.fileTbl.put(file.getName(), file);
	}
	
	/* NON-remote-object-supported methods start from here */
	
	private class DataNodeAbstract implements Comparable<DataNodeAbstract> {

		//TODO:a dataNode stub variable
		private String dataNodeName;
		private String dataNodeRegistryIP;
		private int dataNodeRegistryPort;
		private boolean available;
		private List<String> chunkList;
		private Date latestHeartBeat;
		
		public DataNodeAbstract(String ip, int port, String name) {
			this.chunkList = new ArrayList<String>();
			this.latestHeartBeat = new Date();
			this.dataNodeRegistryIP = ip;
			this.dataNodeRegistryPort = port;
			this.dataNodeName = name;
			this.available = true;
		}

		@Override
		public int compareTo(DataNodeAbstract compareDataNodeInfo) {
			int compareQuantity = compareDataNodeInfo.chunkList.size();
			return this.chunkList.size() - compareQuantity;
		}
		
		private void disableDataNode() {
			this.available = false;
		}
		
		private void enableDataNode() {
			this.available = true;
		}
		
		private boolean isAvailable() {
			return this.available;
		}
		
	}
	
	
	

	private class SystemCheck implements Runnable {
	
		public void run() {
			while (true) {
				HashMap<String, ChunkStatisticsForDataNode> chunkAbstractFromDataNode
					= new HashMap<String, ChunkStatisticsForDataNode>();
				HashMap<String, ChunkStatisticsForNameNode> chunkAbstractFromNameNode
					= new HashMap<String, ChunkStatisticsForNameNode>();

				if (Hdfs.DEBUG) {
					System.out.println("DEBUG NameNode.SystemCheck.run(): Start SystemCheck.");
				}
				
				/* Check availability of DataNode */
				Date now = new Date();
				for (String dataNode : NameNode.this.dataNodeTbl.keySet()) {
					DataNodeAbstract dataNodeInfo = NameNode.this.dataNodeTbl.get(dataNode);
					if ( (now.getTime() - dataNodeInfo.latestHeartBeat.getTime()) > 10 * 60 * 1000) {
						dataNodeInfo.disableDataNode();
					}
				}
				
				/* Obtain chunk abstract from name node */
				Set<String> filePaths = NameNode.this.fileTbl.keySet();
				for (String filePath : filePaths) {
					HDFSFile file = NameNode.this.fileTbl.get(filePath);
					List<HDFSChunk> chunkList = file.getChunkList();
					for (HDFSChunk chunk : chunkList) {
						chunkAbstractFromNameNode.put(chunk.getChunkName(),
								new ChunkStatisticsForNameNode(chunk.getReplicaFactor(), chunk.getChunkName()));
					}
				}
				
				/* Obtain chunk abstract from data node */
				Collection<DataNodeAbstract> dataNodes = NameNode.this.dataNodeTbl.values();
				for (DataNodeAbstract dataNode : dataNodes) {
					if (!dataNode.isAvailable()) {
						continue;
					}
					
					for (String chunkName : dataNode.chunkList) {
						if (chunkAbstractFromDataNode.containsKey(chunkName)) {
							ChunkStatisticsForDataNode chunkStat = chunkAbstractFromDataNode.get(chunkName);
							chunkStat.replicaNum++;
							chunkStat.getLocations().add(dataNode.dataNodeName);
						} else {
							ChunkStatisticsForDataNode chunkStat = new ChunkStatisticsForDataNode();
							chunkStat.replicaNum = 1;
							chunkStat.getLocations().add(dataNode.dataNodeName);
							chunkAbstractFromDataNode.put(chunkName, chunkStat);
						}
					}
				}
				
				/* Delete orphan chunks */
				Set<String> chunksOnDataNode = chunkAbstractFromDataNode.keySet();
				for (String chunkOnDataNode : chunksOnDataNode) {
					if (!chunkAbstractFromNameNode.containsKey(chunkOnDataNode)) {
						System.out.println("DEBUG NameNode.SystemCheck.run(): chunk(" + chunkOnDataNode + ") is orphan.");
						ChunkStatisticsForDataNode chunkStat = chunkAbstractFromDataNode.get(chunkOnDataNode);
						for (String dataNodeName : chunkStat.dataNodes) {
							DataNodeRemoteInterface dataNodeStub = NameNode.this.dataNodeStubTbl.get(dataNodeName);
							try {
								dataNodeStub.deleteChunk(chunkOnDataNode);
							} catch (RemoteException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						
					}
				}
				
				
				/* Compare two abstracts */
				Set<String> chunksOnNameNode = chunkAbstractFromNameNode.keySet();
				for (String chunkOnNameNode : chunksOnNameNode) {
					
					ChunkStatisticsForDataNode chunkStat = null;
					if(chunkAbstractFromDataNode.containsKey(chunkOnNameNode)) {
						chunkStat = chunkAbstractFromDataNode.get(chunkOnNameNode);
					} else {
						//TODO: DISABLE the file
						continue;
					}
					int replicaFac = chunkAbstractFromNameNode.get(chunkOnNameNode).replicaFactor;
					if (chunkStat.replicaNum == replicaFac) {
						System.out.println("DEBUG NameNode.SystemCheck.run(): chunk(" + chunkOnNameNode + ") is OKAY");
					} else if (chunkStat.replicaNum < replicaFac) {
						String debugInfo = String.format("DEBUG NameNode.SystemCheck.run(): chunk(%s) is LESS THAN RF. STAT: num=%d, rf=%d", chunkOnNameNode, chunkStat.replicaNum, replicaFac);
						System.out.println(debugInfo);
					} else {
						String debugInfo = String.format("DEBUG NameNode.SystemCheck.run(): chunk(%s) is MORE THAN RF. STAT: num=%d, rf=%d", chunkOnNameNode, chunkStat.replicaNum, replicaFac);
						System.out.println(debugInfo);
					}
				}
				
				/* TODO:Disable file that has one or more chunks equals to 0 */

				if (Hdfs.DEBUG) {
					System.out.println("DEBUG NameNode.SystemCheck.run(): Finish SystemCheck.");
				}
				try {
					Thread.sleep(1000 * 60);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		private class ChunkStatisticsForNameNode {
			public int replicaFactor;
			public String filePath;
			
			public ChunkStatisticsForNameNode(int rf, String file) {
				this.replicaFactor = rf;
				this.filePath = file;
			}
		}
		
		private class ChunkStatisticsForDataNode {
			public int replicaNum;
			private List<String> dataNodes = new ArrayList<String>();
			
			public List<String> getLocations() {
				return this.dataNodes;
			}
			
		}
		
	}



	
	
}
