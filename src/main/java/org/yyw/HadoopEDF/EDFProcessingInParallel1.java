package org.yyw.HadoopEDF;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.sf.json.JSONObject;

//import org.json.simple.JSONObject;

//import yuanyuan.hadoopEDF.EDFProcessingInParallel1.HadoopEDFMap;

public class EDFProcessingInParallel1 extends Configured implements Tool {

	public static class HadoopEDFMap
    extends Mapper<Text, BytesWritable, Text, Text> {
//set up multiple outputs
/*
private MultipleOutputs<NullWritable,Text> mos;

public void setup(Context context) throws IOException,InterruptedException{
	mos = new MultipleOutputs<NullWritable, Text>(context);
}
*/

//		protected void setup(Context context) throws IOException{
//			
//		}
@SuppressWarnings({ "unchecked", "null" })
@Override
//the (key,value) pair is the input key and value of map, context is the soutput of maps
		protected void map(Text key, BytesWritable value, Context context)
        throws IOException, InterruptedException {    	
	
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();   
            String[] a = key.toString().split(",");
            int startrecord = Integer.valueOf(a[1]);
            int numberOfRecords = Integer.valueOf(a[2]);
	//context.write(new Text("test"), new Text(filename));
    //System.out.println("ggggggg"+EDFFileInputFormat.getInstance().bytesineachrecord.get(filename));

    //System.out.println(key.toString());
/*
    String i= key.toString();
    if(i.equals("shhs1-200001.edf,2,26420")) {
        System.out.println(key);
	    System.out.println(value.getLength());
	   for(byte b:value.getBytes()) {
		   System.out.println(b);
	   }
    }
   
     */
			//Get the URI of the File added to the distributed cache
			URI[] cacheFile = context.getCacheFiles();
			//Get the name of the file added to Distributed cache from the URI
			String filenameofCache=null;
			int lastindex =cacheFile[0].toString().lastIndexOf('/');
			if(lastindex!=-1)
			{
				filenameofCache =cacheFile[0].toString().substring(lastindex+1, cacheFile[0].toString().length());
				
			}
			else
			{
				filenameofCache=cacheFile[0].toString();
			}
	        //Read the content of the distributed file using a Buffered reade
			 BufferedReader reader = null;
		        String laststr = "";
		        try {
		            reader = new BufferedReader(new FileReader(filenameofCache));
		            String tempString = null;
		            int line = 1;
		            while ((tempString = reader.readLine()) != null) {

		                laststr = tempString;
		                line++;
		            }
		            reader.close();
		        } catch (IOException e) {
		            e.printStackTrace();
		        } finally {
		            if (reader != null) {
		                try {
		                    reader.close();
		                } catch (IOException e1) {
		                }
		            }
		        }
		        //convert the content of cache to object which is needed by the following part
		    	JSONObject j1 = JSONObject.fromObject(laststr).getJSONObject(filename);
		        //JSONObject j1 = JSONObject.fromObject(laststr).getJSONObject("shhs1-200002.edf");
		    	int numberOfChannels =j1.getInt("numberOfChannels");
		    	String[] channelLabels = j1.get("ChannelLabels").toString().replace("[", "").replace("]", "").replace(" ", "").split(",");
		 	    Double[] unitsInDigit = new Double[numberOfChannels];
		 	    Double[] dc = new Double[numberOfChannels];
		 	    String[] mi = j1.get("minInUnits").toString().replace("[", "").replace("]", "").split(",");
		 	    Double[] minInUnits = new Double[numberOfChannels];
		 	    for(int i = 0;i<mi.length;i++) {
	    	    	minInUnits[i] = Double.parseDouble(mi[i]);
	    	    	//System.out.println(minInUnits[i]);
	    	    }
		 	    String[] ma = j1.get("maxInUnits").toString().replace("[", "").replace("]", "").split(",");
		 	    Double[] maxInUnits = new Double[numberOfChannels];
		 	    for(int i = 0;i<numberOfChannels;i++) {
	    	    	maxInUnits[i] = Double.parseDouble(ma[i]);
	    	    	//System.out.println(maxInUnits[i]);
	    	    }
		 	    String dmi1 = j1.get("digitalMins").toString().replace("[", "").replace("]", "").replace(" ", "");
		 	    String[] dmi = dmi1.substring(0).split(",");
		 	    Integer[] digitalMin = new Integer[numberOfChannels];
		 	    for(int i = 0;i<numberOfChannels;i++) {
	    	    	digitalMin[i] = Integer.parseInt(dmi[i]);
	    	    	//System.out.println(digitalMin[i]);
	    	    }
		 	    String dma1 = j1.get("digitalMax").toString().replace("[", "").replace("]", "").replace(" ", "");
		 	    String[] dma = dma1.substring(0).split(",");
		 	    Integer[] digitalMax = new Integer[numberOfChannels];
		 	    for(int i = 0;i<dma.length;i++) {
	    	    	digitalMax[i] = Integer.parseInt(dma[i]);
	    	    	//System.out.println(digitalMax[i]);
	    	    }
		 	    String nn = j1.get("numberOfSamples").toString().replace("[", "").replace("]", "").replace(" ", "");
	    	    String[] ns = nn.substring(0).split(",");
		 	    Integer[] numberOfSamples = new Integer[numberOfChannels];
		 	    for(int i = 0;i<ns.length;i++) {
	    	    	numberOfSamples[i] = Integer.parseInt(ns[i]);
	    	    	//System.out.println(numberOfSamples[i]);
	    	    }
		 	    
                short[] sa = new short[value.getLength()/2];
	            ByteBuffer.wrap(value.getBytes()).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(sa);
	 
                for (int i = 0; i < unitsInDigit.length; i++){//unitsInDigit is intialized as the number of channels, should be 14, so i=[0:14]

                    unitsInDigit[i] = (maxInUnits[i] - minInUnits[i])
                                     / (digitalMax[i] - digitalMin[i]);//unitsInDigit is saclefac same as in matlab, actually it unitsInDigit is scale of offset or value of offset.
                	//System.out.println( unitsInDigit[i]);
                    dc[i]=maxInUnits[i] -unitsInDigit[i] *digitalMax[i];//according to the above equation, we also know signal.dc[i]=header.minInUnits[i] - signal.unitsInDigit[i] * header.digitalMin[i]
            }
                    short[][] digitalValues = null;
                    double[][] valuesInUnits = null;
                    digitalValues = new short[numberOfChannels][];
                    valuesInUnits = new double[numberOfChannels][];
                for (int i = 0; i < numberOfChannels; i++)
           {
                    digitalValues[i] = new short[numberOfRecords * numberOfSamples[i]];//such as digitalvalues[1]=10800*1, digitalvalues[2]=10800*125..
                    valuesInUnits[i] = new double[numberOfRecords * numberOfSamples[i]];
           }
 
    int m =0;
    //int fragmentIndex =0;
    //double[] fragmentValue = null;
    //NewMapWritable outputValue = new NewMapWritable();
    //Text outputKey = null;
    //Text outputValue = null;
    //JSONObject jsonObject3 = new JSONObject();
    //Map<Integer,double[]> aa= new LinkedHashMap<Integer,double[]>();
    for (int i = 0; i < numberOfRecords; i++) 
   
            for (int j = 0; j < numberOfChannels; j++) {
            	
                    for (int k = 0; k < numberOfSamples[j]; k++)
                    {
                            int s = numberOfSamples[j] * i + k;
                            //valuesInUnits[j][s] = sa[m] * unitsInDigit[j]+dc[j];
                            digitalValues[j][s] = sa[m];
                            valuesInUnits[j][s] = digitalValues[j][s] * unitsInDigit[j]+dc[j];//this equation is refered by program of matlab              
                            //System.out.println(valuesInUnits[j][s]);
                            valuesInUnits[j][s] = digitalValues[j][s] * unitsInDigit[j]+dc[j];
                            m++;
                    } 
            } 
    
    Text outputkey = null;
    //NewMapWritable immediatevalue = new NewMapWritable();
    JSONObject jsonObject3 = new JSONObject();
    int numberOfSample=0;
   // NewMapWritable immediatevalue = new NewMapWritable();
    for(int i=0;i<numberOfChannels;i++) {
    	long startTimeforjson = System.nanoTime();
    	 double[] channelValues = valuesInUnits[i];
        int fragment=0;
    	numberOfSample=numberOfSamples[i];
    	int length = channelValues.length;
    	int len = length/numberOfSample;
    	//System.out.println(len);
    	for(int j=0;j<len;j++) {
    		double[] ValuePerRecord = new double[numberOfSample];
    		for(int k=0;k<numberOfSample;k++) {
    			ValuePerRecord[k] = channelValues[k+j*numberOfSample];
    			//System.out.println(ValuePerRecord[k]);
    		}
    		int fragmentIndex = startrecord+j;
    		jsonObject3.put(fragmentIndex, Arrays.toString(ValuePerRecord));
        	//immediatevalue.put(new IntWritable((fragment++)+startrecord), new Text(str));
    	}
        System.out.print("\ntotalTimeforjson: " );
        long endTimeforjson   = System.nanoTime();
    	long totalTimeforjson = (endTimeforjson - startTimeforjson)/1000000L;
    	System.out.println(totalTimeforjson + "ms");
      outputkey = new Text(filename+","+channelLabels[i]);
      long startTimefromjosntostring;
      String aa=jsonObject3.toString();
      System.out.print(aa.length());
      Text outputvalue = new Text(jsonObject3.toString());
      
      System.out.print("\ntotalTimefromjosntostring: " );
      long endTimefromjosntostring   = System.nanoTime();
  	 
	//long totalTimefromjosntostring = (endTimefromjosntostring - startTimefromjosntostring)/1000000L;
  	 // System.out.println(totalTimefromjosntostring + "ms");
      context.write(outputkey, outputvalue);
     //outputvalue.put(new IntWritable(startrecord),immediatevalue);
     //context.write(outputkey, immediatevalue);
    /*
    for(int i=0;i<numberOfChannels;i++) {
    	long startTimeforjson = System.nanoTime();
    	 double[] channelValues = valuesInUnits[i];
        int fragment=0;
    	numberOfSample=numberOfSamples[i];
    	int length = channelValues.length;
    	int len = length/numberOfSample;
    	//System.out.println(len);
    	for(int j=0; j<length;j=j+numberOfSample) {
        	String str="";
        	if(numberOfSample==1) {
        		str="["+String.valueOf(channelValues[j])+"]";	
        	}else {
        	for(int k=0; k<numberOfSample; k++) {	
        		if(k==0) {
        			str+="[";
            		str+=String.valueOf(channelValues[j+k]);
            		str+=",";
        		}else if(k==numberOfSample-1) {
            		str+=String.valueOf(channelValues[j+k]);
        			str+="]";
        		}else {
            		str+=String.valueOf(channelValues[j+k]);
            		str+=",";
        		}
        		
        	}
        	}
        	
        	jsonObject3.put((fragment+startrecord), str);
        	fragment+=1;
        	//immediatevalue.put(new IntWritable((fragment++)+startrecord), new Text(str));
    	}
        System.out.print("\ntotalTimeforjson: " );
        long endTimeforjson   = System.nanoTime();
    	long totalTimeforjson = (endTimeforjson - startTimeforjson)/1000000L;
    	System.out.println(totalTimeforjson + "ms");
      outputkey = new Text(filename+","+channelLabels[i]);
      long startTimefromjosntostring = System.nanoTime();
      Text outputvalue = new Text(jsonObject3.toString());
      System.out.print("\ntotalTimefromjosntostring: " );
      long endTimefromjosntostring   = System.nanoTime();
  	  long totalTimefromjosntostring = (endTimefromjosntostring - startTimefromjosntostring)/1000000L;
  	  System.out.println(totalTimefromjosntostring + "ms");
      context.write(outputkey, outputvalue);
     //outputvalue.put(new IntWritable(startrecord),immediatevalue);
     //context.write(outputkey, immediatevalue);
     */    	

}
}
}


public static class HadoopEDFReduce extends Reducer<Text,Text ,NullWritable,Text>{
//set up multiple outputs
private MultipleOutputs<NullWritable,Text> mos;

public void setup(Context context) throws IOException,InterruptedException{
	mos = new MultipleOutputs<NullWritable, Text>(context);
}

@SuppressWarnings("unchecked")
@Override
public void reduce(Text key, Iterable<Text > values, Context context)
    throws IOException, InterruptedException {
	String[] b = key.toString().split(",");
	//System.out.println(key.toString());
    String filename = b[0];
    String channelLabel = b[1];
    long startTimeinreduce = System.nanoTime();
    String output = "";
  
    for(Text value : values) {
    	output+=value.toString();
    }
    Text outputvalue = new Text(output);
    //Text outputvalue = new Text(output.replace("}{", ","));
//    JSONObject jsonObject3 = new JSONObject();
//    for (NewMapWritable value : values) {
//	             Set<Writable> fragmentindexs = value.keySet();
//	             Iterator<Writable> itr=fragmentindexs.iterator();
//	             while(itr.hasNext()) {
//	            	 Writable fragmentindex=(Writable) itr.next();
//	            	 Text ValueOfPerRecord = (Text) value.get(fragmentindex);
//	            	 //DoubleWritable[] vp=(DoubleWritable[]) ValueOfPerRecord.toArray();
//	            	 //jsonObject3.put(fragmentindex, vp);
//	            	
//	            	jsonObject3.put(fragmentindex, ValueOfPerRecord);
//	     
//	            	 }
//	             } 
    System.out.print("\ntotalTimeinreduce: " );
    long endTimeinreduce   = System.nanoTime();
	long totalTimeinreduce = (endTimeinreduce - startTimeinreduce)/1000000L;
	System.out.println(totalTimeinreduce + "ms");
	//Text outputvalue = new Text(jsonObject3.toString());
  //String baseOutputPath=filename+"/";
//if(channelLabel.trim().equals(' ')) {
//	System.out.println("bingo");
//} 
	 long startTimeforReducewrite = System.nanoTime();
try {
//mos.write(channelLabel.trim(), outputkey, outputvalue, "filename/");

mos.write(NullWritable.get(), outputvalue, filename+"/"+channelLabel);
///// mos.write(key, outputvalue,filename+ "/"+key.toString());
System.out.print("\ntotalTimeforReducewrite: " );
long endTimeforReducewrite  = System.nanoTime();
long totalTimeforReducewrite = (endTimeforReducewrite - startTimeforReducewrite)/1000000L;
System.out.println(totalTimeforReducewrite + "ms");
}catch(Exception e){
	System.out.println("bingo");
}
}


public void cleanup(Context context) throws IOException,InterruptedException {
	mos.close();
	}
}


 // context.write(word, new IntWritable(sum));
public static void main(String[] args) {
long startTime = System.nanoTime();
int result = 1;
try {
    result = ToolRunner.run(new EDFProcessingInParallel1(), args);
    System.out.print("\nrunningtime: " );
	long endTime   = System.nanoTime();
	long totalTime = (endTime - startTime)/1000000L;
	//long totalTimeinsecond = totalTime/1000;
	//System.out.println(totalTimeinsecond + "s");
	System.out.println(totalTime + "ms");
} catch (Exception e) {
    e.printStackTrace();
} finally {
    System.exit(result);
}
}


@SuppressWarnings("deprecation")
public int run(String[] args) throws Exception {

if (args.length != 3) {
	 System.err.printf("3 Arguments required\n",getClass().getSimpleName());
	 System.err.println("Example: EDFFileProcessingInParallel <input path> <output path> <input path of the distributed cache>");
	return -1;
	}
Job job = Job.getInstance(getConf(), "EDFProcessingInParallel1");
job.setJarByClass(EDFProcessingInParallel1.class);
//job.setJarByClass(this.getClass());
Configuration conf = job.getConfiguration();
job.setInputFormatClass(EDFFileInputFormat1.class);
FileInputFormat.addInputPath(job,new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.setMapperClass(HadoopEDFMap.class);
job.setReducerClass(HadoopEDFReduce.class);

MultipleOutputs.addNamedOutput(job, "channelLabel", TextOutputFormat.class, NullWritable.class,Text.class);
LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text .class);
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);
job.addCacheFile(new Path(args[2]).toUri());
//DistributedCache.addCacheFile (new Path("/MR-INPUT/distributedtestfile.txt").toUri(), job.getConfiguration());
////job.setNumReduceTasks(4);
//
//Path in = new Path(args[0]);
//Path out = new Path(args[1]);
//
//FileSystem fileSystem = FileSystem.get(new Configuration());
// if (fileSystem.exists(out)) {
//      fileSystem.delete(out, true);
// }
 
//job.setNumReduceTasks(0);

//conf.set("textinputformat.record.delimiter", ".");

return job.waitForCompletion(true) ? 0 : 1;
}
}
	