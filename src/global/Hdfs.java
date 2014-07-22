package global;

/* port range: 1099 - 1148*/
public class Hdfs {
	
	public static class Core {
		
		public static boolean DEBUG = true; //non-configurable

		public static int REPLICA_FACTOR;
		
		public static int CHUNK_SIZE;
		
		public static int WRITE_BUFF_SIZE;
		public static int READ_BUFF_SIZE;
		
		public static int PARTITION_TOLERANCE;
		
		public static String NAME_NODE_SERVICE_NAME = "NameNode"; //non-configurable
		
		public static String DATA_NODE_SERVICE_NAME = "DataNode"; //non-configurable
		
		public static String NAME_NODE_IP;
		
		public static int NAME_NODE_REGISTRY_PORT;
		
		public static String HDFS_TMEP = "tmp";
		
	}
	
	public static class DataNode {
		public static int DATA_NODE_REGISTRY_PORT;
	}

}
