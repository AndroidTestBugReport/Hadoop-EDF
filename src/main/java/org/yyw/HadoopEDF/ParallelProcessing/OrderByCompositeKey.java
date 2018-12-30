package org.yyw.HadoopEDF.ParallelProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import com.google.gson.Gson;

import net.sf.json.JSONObject;

public class OrderByCompositeKey extends Configured implements Tool {

	public static class HadoopEDFMap extends Mapper<Text, BytesWritable, CompositeKey, myMapWritable> {

		private CompositeKey compositeKey = new CompositeKey();

		protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			// Get the URI of the File added to the distributed cache
			URI[] cacheFile = context.getCacheFiles();
			// Get the name of the file added to Distributed cache from the URI
			String filenameofCache = null;
			int lastindex = cacheFile[0].toString().lastIndexOf('/');
			if (lastindex != -1) {
				filenameofCache = cacheFile[0].toString().substring(lastindex + 1, cacheFile[0].toString().length());

			} else {
				filenameofCache = cacheFile[0].toString();
			}
			// Read the content of the distributed file using a Buffered reader
			BufferedReader reader = null;
			String distributedfile = "";
			try {
				reader = new BufferedReader(new FileReader(filenameofCache));
				String tempString = null;
				int line = 1;
				while ((tempString = reader.readLine()) != null) {

					distributedfile = tempString;
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
			// convert the content of cache to object
			String[] a = key.toString().split(",");
			String filename = a[0];
			System.out.println(filename);
			int startrecord = Integer.valueOf(a[1]);
			int numberOfRecords = Integer.valueOf(a[2]);
			JSONObject j1 = JSONObject.fromObject(distributedfile).getJSONObject(filename);
			int numofRecords = j1.getInt("numberOfRecords");
			double durationofRecords = j1.getDouble("durationOfRecords");
			int numberOfChannels = j1.getInt("numberOfChannels");
			String[] channelLabels = j1.get("ChannelLabels").toString().replace("[", "").replace("]", "")
					.replace(" ", "").split(",");

			Double[] dc = new Double[numberOfChannels];
			String[] mi = j1.get("minInUnits").toString().replace("[", "").replace("]", "").split(",");
			Double[] minInUnits = new Double[numberOfChannels];
			for (int i = 0; i < mi.length; i++) {
				minInUnits[i] = Double.parseDouble(mi[i]);
			}
			String[] ma = j1.get("maxInUnits").toString().replace("[", "").replace("]", "").split(",");
			Double[] maxInUnits = new Double[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++) {
				maxInUnits[i] = Double.parseDouble(ma[i]);
			}
			String dmi1 = j1.get("digitalMins").toString().replace("[", "").replace("]", "").replace(" ", "");
			String[] dmi = dmi1.substring(0).split(",");
			Integer[] digitalMin = new Integer[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++) {
				digitalMin[i] = Integer.parseInt(dmi[i]);
			}
			String dma1 = j1.get("digitalMax").toString().replace("[", "").replace("]", "").replace(" ", "");
			String[] dma = dma1.substring(0).split(",");
			Integer[] digitalMax = new Integer[numberOfChannels];
			for (int i = 0; i < dma.length; i++) {
				digitalMax[i] = Integer.parseInt(dma[i]);
			}
			String nn = j1.get("numberOfSamples").toString().replace("[", "").replace("]", "").replace(" ", "");
			String[] ns = nn.substring(0).split(",");
			Integer[] numberOfSamples = new Integer[numberOfChannels];
			for (int i = 0; i < ns.length; i++) {
				numberOfSamples[i] = Integer.parseInt(ns[i]);
			}
			
			Double[] unitsInDigit = new Double[numberOfChannels];
			ByteBuffer bb = ByteBuffer.wrap(value.getBytes());
			
			bb.order(ByteOrder.LITTLE_ENDIAN);
			//convert the raw data to the physical values
			for (int i = 0; i < unitsInDigit.length; i++) {// unitsInDigit is intialized as the number of channels,
															// should be 14, so i=[0:14]
				unitsInDigit[i] = (maxInUnits[i] - minInUnits[i]) / (digitalMax[i] - digitalMin[i]);// unitsInDigit is saclefac same as in matlab, actually it unitsInDigit is scale of offset or value of offset.
				//dc[i] = maxInUnits[i] - unitsInDigit[i] * digitalMax[i];//it is from a tool of the matlab website.https://www.mathworks.com/matlabcentral/fileexchange/31900-edfread
				dc[i] = minInUnits[i] - unitsInDigit[i] * digitalMin[i];// it is from the EDF official website.	https://www.edfplus.info/specs/guidelines.html					
			}	
			double[][] valuesInUnits = null;
			valuesInUnits = new double[numberOfChannels][];
			for (int i = 0; i < numberOfChannels; i++) {
				valuesInUnits[i] = new double[numberOfRecords * numberOfSamples[i]];
			}

			for (int i = 0; i < numberOfRecords; i++) {

				for (int j = 0; j < numberOfChannels; j++) {

					for (int k = 0; k < numberOfSamples[j]; k++) {
						int s = numberOfSamples[j] * i + k;		
						valuesInUnits[j][s] = bb.getShort() * unitsInDigit[j] + dc[j];
					}
				}

			}
			NewMapWritable immediatevalue = new NewMapWritable();
			myMapWritable outputvalue = new myMapWritable();
			for (int n = 0; n < numberOfChannels; n++) {
				String fileChan = filename + "," + channelLabels[n] + "," + numberOfSamples[n].toString() + ","
						+ numofRecords + "," + durationofRecords;
		
				immediatevalue.put(new IntWritable(numberOfRecords), new DoubleArrayWritable(valuesInUnits[n]));
				outputvalue.put(new IntWritable(startrecord), immediatevalue);
				compositeKey.set(fileChan, startrecord);
				context.write(compositeKey, outputvalue);
			}
		}
	}

	public static class HadoopEDFReduce extends Reducer<CompositeKey, myMapWritable, Text, Text> {
		// set up multiple outputs
		private MultipleOutputs<Text, Text> mos;

		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		@Override
		public void reduce(CompositeKey key, Iterable<myMapWritable> values, Context context)
				throws IOException, InterruptedException {

			String[] b = key.fileChan.toString().split(",");

			String filename = b[0];
			String channelLabel = b[1];
			System.out.println(filename+"/"+channelLabel);
			int numberOfSample = Integer.parseInt(b[2]);
			int numOfRecords = Integer.parseInt(b[3]);
			double durationofRecords = Double.parseDouble(b[4]);
			int actualnumOfSample = (int) (numberOfSample / durationofRecords);
			DoubleArrayWritable channelValues = new DoubleArrayWritable();
			Writable[] channelValue = null;
			Text outputkey = null;
			Text outputvalue = null;

			for (myMapWritable value : values) {
				Set<Writable> startrecords = value.keySet();
				if (startrecords.size() == 1) {
					Writable startrecord = startrecords.iterator().next();
					NewMapWritable submap = (NewMapWritable) value.get(startrecord);
					Set<Writable> numberOfRecords = submap.keySet();
					if (numberOfRecords.size() == 1) {
						Writable numberOfRecord = numberOfRecords.iterator().next();
						channelValues = (DoubleArrayWritable) submap.get(numberOfRecord);
						channelValue = channelValues.get();
					}

					int len = channelValue.length / actualnumOfSample;
					for (int i = 0; i < len; i++) {

						Writable[] chanelvalueofeachsecond = new Writable[actualnumOfSample];
						for (int j = 0; j < actualnumOfSample; j++) {
							chanelvalueofeachsecond[j] = channelValue[j + i * actualnumOfSample];
						}
						// In order to get the correct index for each second's values,we have to plus the start of records

						int fragmentindex = Integer.valueOf(startrecord.toString()) + i;

						if (fragmentindex == 1) {
							outputkey = new Text("{" + "\"" + fragmentindex + "\"");
						} else {
							outputkey = new Text("\"" + fragmentindex + "\"");
						}
						if (fragmentindex == numOfRecords) {
						
							outputvalue = new Text("\"" + Arrays.toString(chanelvalueofeachsecond) + "\"" + "}");
						} else {
						
							outputvalue = new Text("\"" + Arrays.toString(chanelvalueofeachsecond) + "\"" + ",");
						}

						mos.write(outputkey, outputvalue, filename + "/" + channelLabel);
						outputvalue=null;
					}
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

	public static void main(String[] args) {
		long startTime = System.nanoTime();
		int result = 1;
		try {
			result = ToolRunner.run(new OrderByCompositeKey(), args);
			System.out.print("\nrunningtime: ");
			long endTime = System.nanoTime();
			long totalTime = (endTime - startTime) / 1000000L;
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
			System.err.printf("3 Arguments required\n", getClass().getSimpleName());
			System.err.println(
					"Example: EDFFileProcessingInParallel <input path> <output path> <input path of the distributed cache>");
			return -1;
		}
		Job job = Job.getInstance(getConf(), "OrderByCompositeKey");
		job.setJarByClass(OrderByCompositeKey.class);
		Configuration conf = job.getConfiguration();
		job.setInputFormatClass(EDFFileInputFormat1.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.addCacheFile(new Path(args[2]).toUri());
		job.setMapperClass(HadoopEDFMap.class);
		job.setReducerClass(HadoopEDFReduce.class);
		// Partitioning/Sorting/Grouping configuration
		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setSortComparatorClass(FullKeyComparator.class);
		job.setGroupingComparatorClass(NaturalKeyComparator.class);
		MultipleOutputs.addNamedOutput(job, "channelLabel", TextOutputFormat.class, Text.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(myMapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//conf.set("mapred.textoutputformat.separator", ":");

		

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
