package mapreduce.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mapreduce.io.KeyValue;
import mapreduce.io.Writable;

public class OutputCollector<K extends Writable, V extends Writable> {
	
	List<KeyValue<K, V>> keyvalueList;
	int partitionNum;
	String jid;
	String tid;
	
	public OutputCollector(int num) {
		this.keyvalueList = new ArrayList<KeyValue<K, V>>();
		this.partitionNum = num;
	}
	
	public void writeToLocal() throws IOException {
		Partitioner<K, V> partitioner = new Partitioner<K, V>();
		OutputCollectorIterator<K, V> it = this.iterator(); 
		int currentPartition = Integer.MAX_VALUE;
		File intermediatePartitionFile = null;
		FileOutputStream outFile = null;
		ObjectOutputStream out = null;


		while (it.hasNext()) {
			KeyValue<K, V> keyvalue = it.next();
			if (currentPartition == partitioner.getPartition(keyvalue.getKey(), keyvalue.getValue(), this.partitionNum)) {
				out.writeObject(keyvalue.getKey());
				out.writeObject(keyvalue.getValue());
			} else {
				if (intermediatePartitionFile != null && outFile != null) {
					out.close();
					outFile.close();
				}
				intermediatePartitionFile = new File(String.format("%s-%s-%s", jid, tid, currentPartition));
				if (!intermediatePartitionFile.exists()) {
					intermediatePartitionFile.createNewFile();
				}
				outFile = new FileOutputStream(intermediatePartitionFile);
				out = new ObjectOutputStream(outFile);
			}
		}
		
		return;
		
	}
	
	public void collect(K key, V value) {
		this.keyvalueList.add(new KeyValue<K, V>(key, value));
	}
	
	public OutputCollectorIterator<K, V> iterator() {
		return new OutputCollectorIterator<K, V>(this.keyvalueList.iterator());
	}
	

	
	private class OutputCollectorIterator<KEY1 extends Writable, VALUE1 extends Writable> {
		Iterator<KeyValue<KEY1, VALUE1>> it;
		
		public OutputCollectorIterator(Iterator<KeyValue<KEY1, VALUE1>> it) {
			this.it = it;
		}
		
		public boolean hasNext() {
			return it.hasNext();
		}
		
		public KeyValue<KEY1, VALUE1> next() {
			return it.next();
		}
	}

	
}
