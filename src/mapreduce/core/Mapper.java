package mapreduce.core;

import java.io.IOException;

import mapreduce.io.collector.OutputCollector;
import mapreduce.io.writable.Writable;

public abstract class Mapper<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	
	public abstract void map(K1 key, V1 value, OutputCollector<K2, V2> output) throws IOException;
}
