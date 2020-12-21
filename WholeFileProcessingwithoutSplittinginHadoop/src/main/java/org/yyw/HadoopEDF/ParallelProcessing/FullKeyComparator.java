package org.yyw.HadoopEDF.ParallelProcessing;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FullKeyComparator extends WritableComparator{
	
	public FullKeyComparator() {
		super(CompositeKey.class, true);
	}
	//This comparator controls the sort order of the keys
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		CompositeKey key1 =(CompositeKey) wc1;
		CompositeKey key2 =(CompositeKey) wc2;
		int fileChanCmp = key1.fileChan.toLowerCase().compareTo(key2.fileChan.toLowerCase());
		if (fileChanCmp == 0) {
			fileChanCmp=Integer.compare(key1.startRecord, key2.startRecord);
		}
		return fileChanCmp;
	}
	

}
