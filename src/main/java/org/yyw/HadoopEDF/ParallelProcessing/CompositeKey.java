package org.yyw.HadoopEDF.ParallelProcessing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {

	public String fileChan;
	//public String ChanLabel;
	public int startRecord;
	
	public CompositeKey() {
	}
	
	public CompositeKey(String fileChan,int startRecord) {
		this.fileChan = (fileChan == null)?"" : fileChan;
		//this.ChanLabel = (ChanLabel == null)?"" : ChanLabel;
		this.startRecord = startRecord;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(fileChan);
		//out.writeUTF(ChanLabel);
		out.writeInt(startRecord);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		fileChan = in.readUTF();
		//ChanLabel = in.readUTF();
		startRecord = in.readInt();
		
	}

	@Override
	public int compareTo(CompositeKey o) {
		// TODO Auto-generated method stub
		int fileChanCmp = fileChan.toLowerCase().compareTo(o.fileChan.toLowerCase());
		if(fileChanCmp == 0) {
			fileChanCmp = Integer.compare(startRecord, o.startRecord);
		}
		return fileChanCmp;//sort ascending
	  //return -1*fileChanCmp //sort descending
	}

	public void set(String outkey, int startrecord2) {
		// TODO Auto-generated method stub
		this.fileChan=outkey;
		this.startRecord=startrecord2;
	}

	public void setfileChan(String fileChan2) {
		// TODO Auto-generated method stub
		this.fileChan = fileChan2;
	}

	public void setstartRecord(int startrecord2) {
		// TODO Auto-generated method stub
		this.startRecord = startrecord2;
	}
}
