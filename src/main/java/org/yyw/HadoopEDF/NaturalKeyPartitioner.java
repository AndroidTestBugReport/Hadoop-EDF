package org.yyw.HadoopEDF;

import org.apache.hadoop.mapreduce.Partitioner;

/*
 * To ensure only the natural key is considered when determing which reducer to send the data to, 
 * we need to write a custom partitioner. The code is straight forward and only considers the filachan
 * of the compositekey class when calculating the reducer the data will be sent to.
 * */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, myMapWritable>{
   
	@Override
	public int getPartition(CompositeKey key, myMapWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		
		return Math.abs(key.fileChan.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
   
}
