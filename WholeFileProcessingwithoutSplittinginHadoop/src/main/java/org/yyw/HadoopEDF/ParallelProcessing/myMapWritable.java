package org.yyw.HadoopEDF.ParallelProcessing;

import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class myMapWritable extends MapWritable {
  
	@Override
	public String toString() {
		String s = new String("{");
		Set<Writable> keys = this.keySet();
		for(Writable key:keys) {
			MapWritable value = (MapWritable) this.get(key);
	        s =s + key.toString() + ","+value.toString();
			}
		s = s+"}";
		return s;
	}
	
}
