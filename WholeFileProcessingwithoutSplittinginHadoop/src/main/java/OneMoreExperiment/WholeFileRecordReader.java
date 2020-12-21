package OneMoreExperiment;



import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WholeFileRecordReader extends RecordReader<Text,FSDataInputStream>  {
	 private FileSplit fileSplit;
	    private JobContext jobContext;
	    private Configuration conf;
	    private Text currentKey = new Text();
	    private FSDataInputStream currentValue;
	    private int processed=0;
	    //private boolean finishConverting = false;
	    public void close() throws IOException {
	        // TODO Auto-generated method stub

	    }

	    public Text getCurrentKey() throws IOException, InterruptedException {
	        // TODO Auto-generated method stub
	        return currentKey;
	    }

	    public FSDataInputStream getCurrentValue() throws IOException,
	            InterruptedException {
	        // TODO Auto-generated method stub
	        return currentValue;
	    }

	    public float getProgress() throws IOException, InterruptedException {
	        // TODO Auto-generated method stub
	        return 0;
	    }
	      

	    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
	            throws IOException, InterruptedException {
	    	//arg0 represents input, arg1 is output
	        this.fileSplit = (FileSplit) arg0;
	        this.jobContext = arg1;
	        this.conf = arg1.getConfiguration();
	    }

	    public boolean nextKeyValue() throws IOException, InterruptedException {
	        // TODO Auto-generated method stub
	        //if(!finishConverting){
	            //int len = (int)fileSplit.getLength();
//	          byte[] content = new byte[len];
	    	
	    	 String filename = fileSplit.getPath().getName();
	         this.currentKey = new Text(filename);
	        
	         if(processed==1){
	            	processed=0;
	            	return false;//false represents that the file has been processed, false = 0,it means file hasn't been processedss when ii=1
	          // byte[] contents = new byte[(int) fileSplit.getLength()];
	         }
	        	 
	        	 Path file = fileSplit.getPath();
	        	 processed=1;
	    	    FileSystem fs = file.getFileSystem(conf);
	    	    FSDataInputStream in = null;
	    	    try{
	    	    	in = fs.open(file);
	    	    	this.currentValue = in;
	    	    	//IOUtils.readFully(in, contents, 0, contents.length);
	    	    	//currentValue.set(contents,0,contents.length);
	    	    }catch(Exception e){
	    	    	System.out.println("It's not inputstream");
	    	    	
	    	    }
	            
	    	    processed=1;
	    	    return true;
	    	   
	    	   
	            //this.currentValue=new Text(file.getName());
	            //return true;
	            
	            }
	            
}
