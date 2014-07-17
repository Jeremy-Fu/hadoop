package global;

public class MapReduce {
	public static boolean DEBUG = true;
	public static boolean UNITEST = false;

	
	public static class JobTracker {
		public static String jobTrackerServiceName = "JobTracker";
		public static String jobTrackerRegistryIp = "localhost";
		public static int MAX_RESCHEDULE_ATTEMPTS = 1;
		public static long TASK_TRACKER_EXPIRATION = 1000 * 60 * 5;
		public static int jobTrackerRegistryPort = 2000;
	}
	
	public static class TaskTracker {
		public static String taskTrackerServiceName = "TaskTracker";
		public static int MAX_NUM_MAP_TASK = 10;
		public static int MAX_NUM_REDUCE_TASK = 3;
		public static int HEART_BEAT_FREQ = 1000; //milliseconds
		public static int BUFF_SIZE = 1024 * 1024;
		public static int taskTrackerServerPort = 8000;
		public static String TEMP_FILE_DIR = "tmp";
	}
	
	public static class TasktTracker1 {
		public static String taskTrackerIp = "localhost";
		public static int taskTrackerPort = 1200;
		public static int taskTrackerServerPort = 8000;
		public static int CORE_NUM = 4;
	}
	
	public static class TasktTracker2 {
		public static String taskTrackerIp = "localhost";
		public static int taskTrackerPort = 1201;
		public static int taskTrackerServerPort = 8001;
		public static int CORE_NUM = 4;
	}
	
	public static class TaskTrackerTest1 {
		public static String fileName = "wordCount";
	}
}
