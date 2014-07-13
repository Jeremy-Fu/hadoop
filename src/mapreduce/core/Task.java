package mapreduce.core;

public class Task {
	String jobId;
	String tid;
	Split split;
	Class<?> taskClass;
	//InputFormat;
	TaskStatus status;
	int partitionNum;
	
	private enum TaskStatus {
		RUNNING, TERMINATED, FAILED;
	}
	
	public Task(String jobId, Split split, Class<?> theClass, int partitionNum) {
		this.jobId = jobId;
		this.split = split;
		this.taskClass = theClass;
		this.partitionNum = partitionNum;
		this.status = TaskStatus.RUNNING;
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	public Split getSplit() {
		return this.split;
	}
	
	public Class<?> getTaskClass() {
		return this.taskClass;
	}
	
	public TaskStatus getTaskStatus() {
		return this.status;
	}
	
	public int getPartitionNum() {
		return this.partitionNum;
	}
}
