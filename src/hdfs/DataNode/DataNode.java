package hdfs.DataNode;

import hdfs.NameNode.NameNodeRemoteInterface;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DataNode implements DataNodeRemoteInterface, Runnable{
	//TODO:Use XML to configure
	private String nameNodeIp;
	private int nameNodePort;
	private int dataNodeName;
	
	public DataNode(String nameNodeIp, int nameNodePort) {
		/* Name Node's RMI registry's address */
		this.nameNodeIp = nameNodeIp;
		this.nameNodePort = nameNodePort;
	}
	
	/**
	 * Initialize this Data Node by binding the remote object
	 */
	public void init() {
		try {
			Registry localRegistry = LocateRegistry.createRegistry(1100);
			DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
			localRegistry.rebind("DataNode", dataNodeStub);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		try {			
			/* join hdfs cluster */
			Registry registryOnNameNode = LocateRegistry.getRegistry(nameNodeIp, nameNodePort);
			NameNodeRemoteInterface nameNode = (NameNodeRemoteInterface) registryOnNameNode.lookup("NameNode");
			this.dataNodeName = nameNode.join(InetAddress.getLocalHost().getHostAddress(), 1100);
			
			while (true) {
				Thread.sleep(10000);
				nameNode.heartBeat(dataNodeName);
			}
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
