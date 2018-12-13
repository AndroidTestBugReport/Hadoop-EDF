package org.yyw.HadoopEDF;

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

//import org.json.simple.JSONObject;

//import yuanyuan.hadoopEDF.EDFProcessingInParallel1.HadoopEDFMap;

public class OrderByCompositeKey extends Configured implements Tool {

	// public static final Log LOG = LogFactory.getLog(OrderByCompositeKey.class);

	public static class HadoopEDFMap extends Mapper<Text, BytesWritable, CompositeKey, myMapWritable> {

		private CompositeKey compositeKey = new CompositeKey();

		protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			// String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			// if(filename.equals("shhs1-200002.edf")) {
			// System.out.println(filename);
			// }
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
			// Read the content of the distributed file using a Buffered reade
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
			// convert the content of cache to objec
			String[] a = key.toString().split(",");
			String filename = a[0];
			System.out.println(filename);
			int startrecord = Integer.valueOf(a[1]);
			int numberOfRecords = Integer.valueOf(a[2]);
			JSONObject j1 = JSONObject.fromObject(distributedfile).getJSONObject(filename);
			// JSONObject j1 =
			// JSONObject.fromObject(laststr).getJSONObject("shhs1-200002.edf");
			int numofRecords = j1.getInt("numberOfRecords");
			// System.out.println(numofRecords);
			double durationofRecords = j1.getDouble("durationOfRecords");
			// System.out.println(durationofRecords);
			int numberOfChannels = j1.getInt("numberOfChannels");
			// System.out.println("filename: "+filename);
			// System.out.println("numberOfChannels: "+numberOfChannels);
			String[] channelLabels = j1.get("ChannelLabels").toString().replace("[", "").replace("]", "")
					.replace(" ", "").split(",");

			Double[] dc = new Double[numberOfChannels];
			String[] mi = j1.get("minInUnits").toString().replace("[", "").replace("]", "").split(",");
			Double[] minInUnits = new Double[numberOfChannels];
			for (int i = 0; i < mi.length; i++) {
				minInUnits[i] = Double.parseDouble(mi[i]);
				// System.out.println(minInUnits[i]);
			}
			String[] ma = j1.get("maxInUnits").toString().replace("[", "").replace("]", "").split(",");
			Double[] maxInUnits = new Double[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++) {
				maxInUnits[i] = Double.parseDouble(ma[i]);
				// System.out.println(maxInUnits[i]);
			}
			String dmi1 = j1.get("digitalMins").toString().replace("[", "").replace("]", "").replace(" ", "");
			String[] dmi = dmi1.substring(0).split(",");
			Integer[] digitalMin = new Integer[numberOfChannels];
			for (int i = 0; i < numberOfChannels; i++) {
				digitalMin[i] = Integer.parseInt(dmi[i]);
				// System.out.println(digitalMin[i]);
			}
			String dma1 = j1.get("digitalMax").toString().replace("[", "").replace("]", "").replace(" ", "");
			String[] dma = dma1.substring(0).split(",");
			Integer[] digitalMax = new Integer[numberOfChannels];
			for (int i = 0; i < dma.length; i++) {
				digitalMax[i] = Integer.parseInt(dma[i]);
				// System.out.println(digitalMax[i]);
			}
			String nn = j1.get("numberOfSamples").toString().replace("[", "").replace("]", "").replace(" ", "");
			String[] ns = nn.substring(0).split(",");
			Integer[] numberOfSamples = new Integer[numberOfChannels];
			for (int i = 0; i < ns.length; i++) {
				numberOfSamples[i] = Integer.parseInt(ns[i]);
				// System.out.println(numberOfSamples[i]);
			}
			// int bytesineachrecord = j1.getInt("bytesineachrecord");
			Double[] unitsInDigit = new Double[numberOfChannels];
			// int lenofvalue =numberOfRecords*bytesineachrecord;
			// value.setCapacity(lenofvalue);
			// value.setCapacity(value.getSize());
			// byte[] issue=Arrays.copyOf(value.getBytes(),lenofvalue);
			// byte[] issue = value.copyBytes();

			// value.

			// System.out.println("issue20000+"+20000);
			// System.out.println("issue24000+"+issue[24000]);
			// System.out.println("issue38000+"+issue[38000]);
			// System.out.println("issue41000+"+issue[410000]);

			// System.out.print(issue[16000]+","+issue[16001]+","+issue[16002]+","+issue[16003]+","+issue[16004]+","+issue[16005]);
			ByteBuffer bb = ByteBuffer.wrap(value.getBytes());
			// ByteBuffer bb = ByteBuffer.wrap(issue);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			//
			for (int i = 0; i < unitsInDigit.length; i++) {// unitsInDigit is intialized as the number of channels,
															// should be 14, so i=[0:14]

				unitsInDigit[i] = (maxInUnits[i] - minInUnits[i]) / (digitalMax[i] - digitalMin[i]);// unitsInDigit is
																									// saclefac same as
																									// in matlab,
																									// actually it
																									// unitsInDigit is
																									// scale of offset
																									// or value of
																									// offset.
				// System.out.println( unitsInDigit[i]);
				
				//dc[i] = maxInUnits[i] - unitsInDigit[i] * digitalMax[i];//it is from a tool of the matlab website.
				dc[i] = minInUnits[i] - unitsInDigit[i] * digitalMin[i];// it is from the EDF official website.						
			
			}
			// short[][] digitalValues = null;
			double[][] valuesInUnits = null;
			/// digitalValues = new short[numberOfChannels][];
			valuesInUnits = new double[numberOfChannels][];
			for (int i = 0; i < numberOfChannels; i++) {
				// digitalValues[i] = new short[numberOfRecords * numberOfSamples[i]];//such as
				// digitalvalues[1]=10800*1, digitalvalues[2]=10800*125..
				valuesInUnits[i] = new double[numberOfRecords * numberOfSamples[i]];
			}

			// int m =0;
			// System.out.print("the length of sa: "+sa.length);
			// System.out.print("numberOfRecords in mapper: "+numberOfRecords);
			for (int i = 0; i < numberOfRecords; i++) {

				for (int j = 0; j < numberOfChannels; j++) {

					for (int k = 0; k < numberOfSamples[j]; k++) {
						int s = numberOfSamples[j] * i + k;
						// digitalValues[j][s] = bb.getShort();
						// valuesInUnits[j][s] = digitalValues[j][s];//s=13 j=0
						// valuesInUnits[j][s] = digitalValues[j][s] * unitsInDigit[j]+dc[j];
						valuesInUnits[j][s] = bb.getShort() * unitsInDigit[j] + dc[j];
						// System.out.println(j+","+s+","+valuesInUnits[j][s]);
						// valuesInUnits[j][s] = sa[m] * unitsInDigit[j]+dc[j];
						// // System.out.println(m+","+sa[m]+","+valuesInUnits[0][0]);
						// //System.out.println(sa[m]+","+unitsInDigit[j]+","+dc[j]+","+(sa[m] *
						// unitsInDigit[j]+dc[j])+","+j+","+s+","+valuesInUnits[j][s]);

						// m++;
						//

						// valuesInUnits[j][s] = digitalValues[j][s] * unitsInDigit[j]+dc[j];//this
						// equation is refered by program of matlab
						// System.out.print(channelLabels[j]);
						// System.out.println(valuesInUnits[j][s]);
						// valuesInUnits[j][s] = digitalValues[j][s] * unitsInDigit[j]+dc[j];

					}

				}

			}
			// System.out.print(channelLabels[0]+"ValuesInUnits:
			// "+valuesInUnits[0][12]+","+valuesInUnits[0][13]+","+valuesInUnits[0][14]+","+valuesInUnits[0][15]);

			// double[] channelValues = null;
			Text outputkey = null;

			NewMapWritable immediatevalue = new NewMapWritable();
			myMapWritable outputvalue = new myMapWritable();
			for (int n = 0; n < numberOfChannels; n++) {
				String fileChan = filename + "," + channelLabels[n] + "," + numberOfSamples[n].toString() + ","
						+ numofRecords + "," + durationofRecords;
				// outputkey = new Text(outkey);
				// channelValues = valuesInUnits[n];
				// System.out.print(channelLabels[n]+valuesInUnits[n][12]+","+valuesInUnits[n][13]+","+valuesInUnits[n][14]+","+valuesInUnits[n][15]);
				immediatevalue.put(new IntWritable(numberOfRecords), new DoubleArrayWritable(valuesInUnits[n]));
				outputvalue.put(new IntWritable(startrecord), immediatevalue);
				// compositeKey.setfileChan(fileChan);
				// compositeKey.setstartRecord(startrecord);
				compositeKey.set(fileChan, startrecord);
				// System.out.println(compositeKey.startRecord);
				context.write(compositeKey, outputvalue);
				//valuesInUnits=null;
				//System.gc();
				// System.out.println("-----------bingo---------");
				// context.write(compositeKey, outputvalue);
			}
		}
	}

	public static class HadoopEDFReduce extends Reducer<CompositeKey, myMapWritable, Text, Text> {
		// set up multiple outputs
		private MultipleOutputs<Text, Text> mos;

		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		@SuppressWarnings({ "unchecked", "null" })
		@Override
		public void reduce(CompositeKey key, Iterable<myMapWritable> values, Context context)
				throws IOException, InterruptedException {

			String[] b = key.fileChan.toString().split(",");
			
//			int startRecord = key.startRecord;
//			System.out.println(startRecord);
			// System.out.println(key.toString());
			String filename = b[0];
			String channelLabel = b[1];
			System.out.println(filename+"/"+channelLabel);
			int numberOfSample = Integer.parseInt(b[2]);
			int numOfRecords = Integer.parseInt(b[3]);
			double durationofRecords = Double.parseDouble(b[4]);
			// double durationofRecords = 0.1;
			int actualnumOfSample = (int) (numberOfSample / durationofRecords);
			// System.out.println(actualnumOfSample );
			DoubleArrayWritable channelValues = new DoubleArrayWritable();
			// DoubleWritable[] channelValue = null;
			Writable[] channelValue = null;
			Text outputkey = null;
			Text outputvalue = null;

			for (myMapWritable value : values) {
				Set<Writable> startrecords = value.keySet();
				if (startrecords.size() == 1) {
					Writable startrecord = startrecords.iterator().next();
					// System.out.println("startrecordinreduce: "+startrecord);
					NewMapWritable submap = (NewMapWritable) value.get(startrecord);
					Set<Writable> numberOfRecords = submap.keySet();
					if (numberOfRecords.size() == 1) {
						Writable numberOfRecord = numberOfRecords.iterator().next();
						channelValues = (DoubleArrayWritable) submap.get(numberOfRecord);
						channelValue = channelValues.get();
						// channelValue = (DoubleWritable[]) channelValues.toArray();
					}

					int len = channelValue.length / actualnumOfSample;

					// divide channel values to the value of each second by the number of samples of
					// the certain channel
					for (int i = 0; i < len; i++) {
						// System.out.println(i);
						Writable[] chanelvalueofeachsecond = new Writable[actualnumOfSample];
						// DoubleWritable[] chanelvalueofeachsecond = new
						// DoubleWritable[actualnumOfSample];
						// System.out.println(chanelvalueofeachsecond.length);
						for (int j = 0; j < actualnumOfSample; j++) {
							chanelvalueofeachsecond[j] = channelValue[j + i * actualnumOfSample];
							// System.out.println(chanelvalueofeachsecond[j]);
						}
						// In order to get the correct index for each second's values,we have to plus
						// the start of records

						int fragmentindex = Integer.valueOf(startrecord.toString()) + i;

						if (fragmentindex == 1) {
							outputkey = new Text("{" + "\"" + fragmentindex + "\"");
						} else {
							outputkey = new Text("\"" + fragmentindex + "\"");
						}
						if (fragmentindex == numOfRecords) {
							// System.out.println(Arrays.toString(chanelvalueofeachsecond));
							outputvalue = new Text("\"" + Arrays.toString(chanelvalueofeachsecond) + "\"" + "}");
						} else {
							// outputvalue = new Text("\""+Arrays.toString(chanelvalueofeachsecond)+"\"");
							outputvalue = new Text("\"" + Arrays.toString(chanelvalueofeachsecond) + "\"" + ",");
						}

						// outputvalue = new DoubleArrayWritable(chanelvalueofeachsecond);
						// String channel_directory =filename+"/"+channelLabel;

						// mos.write(outputkey, outputvalue, channel_directory);

						mos.write(outputkey, outputvalue, filename + "/" + channelLabel);
						outputvalue=null;
						//System.gc();
						// jsonObject3.put(fragmentindex, Arrays.toString(chanelvalueofeachsecond));
						// jsonObject3.put(fragmentindex, Arrays.deepToString(chanelvalueofeachsecond));
						// jsonObject3.putAll(SignalValue1);
						// finalValue.put(new IntWritable(fragmentindex), new
						// DoubleArrayWritable(chanelvalueofeachsecond));
					}
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

	// context.write(word, new IntWritable(sum));
	public static void main(String[] args) {
		long startTime = System.nanoTime();
		int result = 1;
		try {
			result = ToolRunner.run(new OrderByCompositeKey(), args);
			System.out.print("\nrunningtime: ");
			long endTime = System.nanoTime();
			long totalTime = (endTime - startTime) / 1000000L;
			// long totalTimeinsecond = totalTime/1000;
			// System.out.println(totalTimeinsecond + "s");
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
		// job.setJarByClass(this.getClass());
		Configuration conf = job.getConfiguration();
		job.setInputFormatClass(EDFFileInputFormat1.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(HadoopEDFMap.class);
		job.setReducerClass(HadoopEDFReduce.class);
		// Partitioning/Sorting/Grouping configuration
		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setSortComparatorClass(FullKeyComparator.class);
		job.setGroupingComparatorClass(NaturalKeyComparator.class);
		MultipleOutputs.addNamedOutput(job, "channelLabel", TextOutputFormat.class, Text.class, Text.class);
		// MultipleOutputs.addNamedOutput(job, "channelLabel",OverwriteTextOutputFormat.class,Text.class,Text.class);
		//LazyOutputFormat.setOutputFormatClass(job, OverwriteTextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(myMapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		conf.set("mapred.textoutputformat.separator", ":");
		
//		conf.set("mapreduce.map.memory.mb", "9216");
//		conf.set("mapreduce.map.java.opts", "-Xmx7168m");
//		conf.set("mapreduce.reduce.memory.mb", "9216");
//		conf.set("mapreduce.reduce.java.opts", "-Xmx7168m");
		// conf.setBoolean("mapred.output.compress",true);
		// conf.setBooleanIfUnset("mapred.output.compression.codec", value);
	
//		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
//		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
//		conf.setBoolean("mapred.map.output.compress", true);
		
//		//10.30
//		Path path=new Path(args[1]);
//		path.getFileSystem(conf).delete(path, true);
		
		// conf.set("mapreduce.cluster.temp.dir", "${hadoop.tmp.dir}/mapred/temp");
		// conf.setBoolean("mapreduce.output.fileoutputformat.compress", false);
		job.addCacheFile(new Path(args[2]).toUri());
		//// DistributedCache.addCacheFile (new Path(args[2]).toUri(),
		//// job.getConfiguration());
		// job.setNumReduceTasks(10);
		// job.setNumReduceTasks(20);
		// job.setNumReduceTasks(30);
		// FileSystem.get(conf).delete(new Path(args[1]),true);
		// FileSystem fs = FileSystem.get(new Configuration());
		// FSDataOutputStream fileOut=fs.create(new Path(args[1]), true);

		// Path in = new Path(args[0]);
//		 Path out = new Path(args[1]);
//		// //Path distributed = new Path(args[2]);
//		// //System.out.println(distributed.toString());
//		//
//		 FileSystem fileSystem = FileSystem.get(new Configuration());
//		//
//		 if (fileSystem.exists(out)) {
//		 fileSystem.delete(out, true);
//		 }

		// job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
