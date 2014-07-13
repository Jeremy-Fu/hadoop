package mapreduce;

import java.util.List;

import mapreduce.core.Split;

public class Job {
	private String jobId;
	private JobConf conf;
	private List<Split> splits;
	
	public Job(JobConf conf) {
		this.conf = conf;
	}
	
	public void setJobId(String id) {
		this.jobId = id;
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	public JobConf getJobConf() {
		return this.conf;
	}
	
	public void setSplit(List<Split> splits) {
		this.splits = splits;
	}
	
	public List<Split> getSplit() {
		return this.splits;
	}
}