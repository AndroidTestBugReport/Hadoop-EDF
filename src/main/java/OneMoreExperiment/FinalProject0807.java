package OneMoreExperiment;


import java.io.IOException;

import java.util.*;


import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParser;
import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParserResult;



public class FinalProject0807 extends Configured implements Tool {
	public static class HadoopEDFMap extends Mapper<Text, FSDataInputStream, Text, Text> {
		// set up multiple outputs
		private MultipleOutputs<Text, Text> mos;

		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		// the (key,value) pair is the input key and value of map, context is the
		// soutput of maps
		protected void map(Text key, FSDataInputStream value, Context context)
				throws IOException, InterruptedException {

			//String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
         
			//////////// start input to EDFParser
			String filename = key.toString();
			EDFParserResult result = EDFParser.parseEDF(value);
			value.close();
	
			//int numberOfRecords = result.getHeader().getNumberOfRecords();
			double durationOfRecords = result.getHeader().getDurationOfRecords();
			int numberOfChannels = result.getHeader().getNumberOfChannels();
			String[] channelLabels = result.getHeader().getChannelLabels();
			Integer[] numberOfSamples = result.getHeader().getNumberOfSamples();
			int numberOfRecords = result.getHeader().getNumberOfRecords();
			
			double[][] valuesInUnits = result.getSignal().getValuesInUnits();
			Text outputkey = null;
			Text outputvalue = null;
			for (int i_sig = 0; i_sig < numberOfChannels; i_sig++) {
				int actualnumOfSample = (int) (numberOfSamples[i_sig] / durationOfRecords);
				double[] channelValue = valuesInUnits[i_sig];
				int len = channelValue.length / actualnumOfSample;
				int fragmentindex=1;
				for (int i = 0; i < len; i++) {
					double[] chanelvalueofeachsecond = new double[actualnumOfSample];
					for (int j = 0; j < actualnumOfSample; j++) {
						chanelvalueofeachsecond[j] = channelValue[j + i * actualnumOfSample];
					}
					// In order to get the correct index for each second's values,we have to plus
					// the start of records

					if (fragmentindex == 1) {
						//outputkey = new Text("{" + "\"" + fragmentindex + "\""+":");
						outputkey = new Text("{" + "\"" + fragmentindex + "\"");
					} else {
						//outputkey = new Text("\""+ fragmentindex + "\""+":");
						outputkey = new Text("\""+ fragmentindex + "\"");
					}
					if (fragmentindex == numberOfRecords) {
						outputvalue = new Text("\"" + Arrays.toString(chanelvalueofeachsecond) + "\"" + "}");
					} else {
						outputvalue = new Text("\"" + Arrays.toString(chanelvalueofeachsecond) + "\"" + ",");
					}

					mos.write(outputkey, outputvalue, filename + "/" + channelLabels[i_sig].trim());
					fragmentindex = fragmentindex+1;
					//control memory to avoid the exceed of the used memory
					//outputvalue = null;
					//result = null;
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
			result = ToolRunner.run(new FinalProject0807(), args);
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

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "FinalProject0807");
		job.setJarByClass(FinalProject0807.class);

		Configuration conf = job.getConfiguration();
	
		job.setInputFormatClass(WholeFileInputFormat.class);
		// job.setInputFormatClass(WholeFileInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(HadoopEDFMap.class);
		//conf.set("mapred.textoutputformat.separator", ":");
		conf.set("mapreduce.output.textoutputformat.separator", ":");
		//conf.set("mapreduce.reduce.memory.mb", "2457");
		//conf.set("mapreduce.map.java.opts", "-Xmx10240m");
		
		//conf.set("mapreduce.output.key.field.separator", ":");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// Text.class is the datatype of output key, InnWritable.class is the datatype
		// of output value

		MultipleOutputs.addNamedOutput(job, "ConvertedEdfFile", TextOutputFormat.class, Text.class, Text.class);
		/*
		 * In default, the empty file of part-r-00000 will be generated, when you use
		 * multipleOutputs class. The following method can cancel the generation of
		 * part-r-00000 file.
		 */
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
        
	
//		Path out = new Path(args[1]);
//		FileSystem fileSystem = FileSystem.get(new Configuration());
//		 if (fileSystem.exists(out)) {
//		      fileSystem.delete(out, true);
//		 }
		// job.setNumReduceTasks(0);

		// conf.set("textinputformat.record.delimiter", ".");

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
