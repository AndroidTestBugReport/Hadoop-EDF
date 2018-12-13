package org.yyw.HadoopEDF;



import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileStream.Header;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class EDFFileRecordReader1 extends RecordReader<Text,BytesWritable> {
	
	private FileSplit fileSplit;
    private JobContext jobContext;
    private Configuration conf;
    private Text currentKey = new Text();
    //private Text currentValue;
    private BytesWritable currentValue;
    private int processed=0;
    private long start;
    private long len;
    private long end;
    private int startOfRecords;
	private FSDataInputStream In = null;  
	public int length;
	private int numberofrecords;
	//private int numberofchannels;
	private int bytesinHeader;
	//private double durationofdatarecords=0;
	
	//private int i;
	private int numofrecordsforonesplit;
	private int bytesineachrecord;
	
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		    this.fileSplit = (FileSplit) split;
	        this.jobContext = context;
	        this.conf = context.getConfiguration();
	        //read file behind header, so 6 is the length of header.
        	Path file = fileSplit.getPath();
            System.out.println(file);
    	    FileSystem fs = file.getFileSystem(conf);
    	    In = fs.open(file);
    	  //System.out.println(In.available());
    	    start = fileSplit.getStart();
    	    len = fileSplit.getLength();
    	    //System.out.println(len);
    	    end = start + len;
    	    //System.out.println("start location: "+start);
    	    //System.out.println("end location: "+end);	 
    	   
    	    //System.out.println(buffer.toString());
    	 
    	    EDFParserResult result = EDFParser.parseHeader(In);
    	    bytesinHeader = result.getHeader().getBytesInHeader();
    	    //numberofchannels = result.getHeader().getNumberOfChannels();
    	    Integer[] numberOfSamples = result.getHeader().getNumberOfSamples(); 
    	    Integer sum = 0;
    	   	   for (Integer numofsample : numberOfSamples) {
    	   		   sum = sum + numofsample;   
    	     	}
    	   	bytesineachrecord = sum *2 ;
    	   	//System.out.print("total number of samples"+sum);
	    	length = bytesineachrecord;
	    	numberofrecords=result.getHeader().getNumberOfRecords(); 
	    	//System.out.println("numberofrecords in record reader: "+numberofrecords);
	}

	
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
  	
    	 if(processed==1){
         	processed=0;
         	return false;//this justify is able to get current key and value from the nexKeyValue() method
      }
        // TODO Auto-generated method stub
    	//key is filename, value is the corresponding content of each fixed length
            String filename = fileSplit.getPath().getName();
            //EDFFileInputFormat a = new EDFFileInputFormat();
            //System.out.println(a.segment);
            //System.out.println(EDFFileInputFormat.bytesineachrecord);
            //System.out.println("ggggggg"+EDFFileInputFormat.getInstance().bytesineachrecord.get(filename));
            numofrecordsforonesplit = (int) (len/bytesineachrecord); 
            //System.out.println(" num of records for one split in record reader: "+numofrecordsforonesplit);
            
            if(start==(long)bytesinHeader) {
            	 startOfRecords=1;
            	 //endOfRecords=numofrecordsforonesplit;
            }else if((start-(long)bytesinHeader)>0){
            	 startOfRecords=(int)((start-(long)bytesinHeader)/bytesineachrecord)+1;
           	     //endOfRecords=startOfRecords+numofrecordsforonesplit-1;
            }
            currentKey.set(filename+","+startOfRecords+","+numofrecordsforonesplit);
            //System.out.println("currentkey in record reader: "+currentKey);
            byte[] buffer = new byte[(int) len];
            //System.out.println(In.available());
            In.readFully(start, buffer, 0, (int) len);
    	    //int lenofsplit=In.read(start, buffer, 0, (int) len);
//    	  	short[] sa = new short[(int) len/2];
//    	    ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(sa);
//    	    System.out.println("short values "+sa[12]+","+sa[13]+","+sa[14]+","+sa[15]);
    	    //System.out.println("values "+buffer[12000]+","+buffer[23000]+","+buffer[240000]+","+buffer[40000]+","+buffer[42000]);
    	    currentValue = new BytesWritable(buffer);
    	    processed = 1;
    	    In.close();
    	    //System.gc();
    	    return true;
    	   
    }
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return currentKey;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        return currentValue;
    }

    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
    	float aa=1;
    	return aa;
    	}
    }


