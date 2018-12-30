package org.yyw.HadoopEDF.ParallelProcessing;



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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParser;
import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParserResult;



public class EDFFileRecordReader1 extends RecordReader<Text,BytesWritable> {
	
	private FileSplit fileSplit;
    private JobContext jobContext;
    private Configuration conf;
    private Text currentKey = new Text();
    private BytesWritable currentValue;
    private int processed=0;
    private long start;
    private long len;
    private long end;
    private int startOfRecords;
	private FSDataInputStream In = null;  
	public int length;
	private int numberofrecords;
	private int bytesinHeader;
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
	        //read file behind header.
        	Path file = fileSplit.getPath();
            System.out.println(file);
    	    FileSystem fs = file.getFileSystem(conf);
    	    In = fs.open(file);
    	    start = fileSplit.getStart();
    	    len = fileSplit.getLength();
    	    end = start + len;
    	    EDFParserResult result = EDFParser.parseHeader(In);
    	    bytesinHeader = result.getHeader().getBytesInHeader();
    	    Integer[] numberOfSamples = result.getHeader().getNumberOfSamples(); 
    	    Integer sum = 0;
    	   	   for (Integer numofsample : numberOfSamples) {
    	   		   sum = sum + numofsample;   
    	     	}
    	   	bytesineachrecord = sum *2 ;
	    	length = bytesineachrecord;
	    	numberofrecords=result.getHeader().getNumberOfRecords(); 
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
            numofrecordsforonesplit = (int) (len/bytesineachrecord); 
            
            if(start==(long)bytesinHeader) {
            	 startOfRecords=1;
            }else if((start-(long)bytesinHeader)>0){
            	 startOfRecords=(int)((start-(long)bytesinHeader)/bytesineachrecord)+1;
            }
            currentKey.set(filename+","+startOfRecords+","+numofrecordsforonesplit);
            byte[] buffer = new byte[(int) len];
            In.readFully(start, buffer, 0, (int) len);
    	    currentValue = new BytesWritable(buffer);
    	    processed = 1;
    	    In.close();
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


