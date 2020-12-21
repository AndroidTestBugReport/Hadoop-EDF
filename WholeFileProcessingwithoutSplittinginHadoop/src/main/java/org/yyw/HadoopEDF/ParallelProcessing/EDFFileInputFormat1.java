package org.yyw.HadoopEDF.ParallelProcessing;

import java.io.IOException;

import java.util.ArrayList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParser;
import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParserResult;

import net.sf.json.JSONObject;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class EDFFileInputFormat1 extends FileInputFormat<Text, BytesWritable> {

	private static final double SPLIT_SLOP = 1; // 10% slop
	private int numofsplits;
	private int bytesindatarecord;
	private String idCode;
	private String subjectID;
	private String recordingID;
	private String startDate;
	private String startTime;
	private int bytesinheader;
	private String reserved;
	private double durationOfRecords;
	private Integer NumberOfRecords;
	private Integer NumberOfChannels;
	private Integer[] numberOfSamples;
	private int numofrecordsforeachsplit;
	private Integer bytesineachrecord;
	private int blkIndex;
	private String filename;

	@SuppressWarnings("unchecked")
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<FileStatus> files = listStatus(job);
		// Save the number of input files in the job-conf
		// return all files of the input folder, it will throw error if that input folder contains a sub-directory
		// get header information
		for (FileStatus file : files) {
			if (file.isDirectory()) {
				throw new IOException("Not a file: " + file.getPath());
			}
		}

		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>(numofsplits);

		for (FileStatus file : files) {
			// calculate the number of all input files which is 2 now
			Path path = file.getPath();
			//System.out.println(path);
			filename = path.getName();
			Configuration conf = job.getConfiguration();
			FileSystem fs = path.getFileSystem(conf);
			FSDataInputStream in = null;

			Path outputDir = FileOutputFormat.getOutputPath((JobConf) conf);
			String EDFHeader1 = null;
	
			try {
				in = fs.open(path);
				// process header record and get the header information
				EDFParserResult result = EDFParser.parseHeader(in);
				bytesinheader = result.getHeader().getBytesInHeader();	
				NumberOfRecords = result.getHeader().getNumberOfRecords();
				NumberOfChannels = result.getHeader().getNumberOfChannels();
				numberOfSamples = result.getHeader().getNumberOfSamples();
				idCode = result.getHeader().getIdCode().trim();
				recordingID = subjectID = result.getHeader().getSubjectID().trim();
				startDate = result.getHeader().getStartDate().trim();
				startTime = result.getHeader().getStartTime().trim();
				reserved = result.getHeader().getFormatVersion().trim();
				durationOfRecords = result.getHeader().getDurationOfRecords();

				Integer sum = 0;
				for (Integer numofsample : numberOfSamples) {
					sum = sum + numofsample;
				}

				bytesineachrecord = sum * 2;
				// output EDF Header information in Json format.
				String sharefile = outputDir.toString() + "/" + filename + "/";
				String file1 = sharefile + "EDFHeader.json";
				String file2 = sharefile + "SignalHeader.json";

				JSONObject jsonObject1 = new JSONObject();
				WritetoHDFS w = new WritetoHDFS();
				jsonObject1.put("idCode", idCode);
				jsonObject1.put("subjectID", subjectID);
				jsonObject1.put("recordingID", recordingID);
				jsonObject1.put("startDate", startDate);
				jsonObject1.put("startTime", startTime);
				jsonObject1.put("bytesInHeader", bytesinheader);
				jsonObject1.put("formatVersion", reserved);
				jsonObject1.put("numberOfRecords", NumberOfRecords);
				jsonObject1.put("durationOfRecords", durationOfRecords);
				jsonObject1.put("numberOfChannels", NumberOfChannels);

				w.WriteToHDFS(file1, jsonObject1.toString());
				jsonObject1.clear();

				String[] channelLabels = result.getHeader().getChannelLabels();
				String[] transducerTypes = result.getHeader().getTransducerTypes();
				String[] dimensions = result.getHeader().getDimensions();
				Double[] minInUnits = result.getHeader().getMinInUnits();
				Double[] maxInUnits = result.getHeader().getMaxInUnits();
				Integer[] digitalMin = result.getHeader().getDigitalMin();
				Integer[] digitalMax = result.getHeader().getDigitalMax();
				String[] prefilterings = result.getHeader().getPrefilterings();
				byte[][] reserved_area = result.getHeader().getReserveds();
				OutputSignalHeader SignalHeader = new OutputSignalHeader();

				Map<String, Object> SignalHeader1 = new LinkedHashMap<String, Object>();
				JSONObject jsonObject3 = new JSONObject();
				Integer total_samples;

				for (int i = 0; i < NumberOfChannels; i++) {
					total_samples = numberOfSamples[i] * NumberOfRecords;
					SignalHeader1 = SignalHeader.OutputSignalHeader(channelLabels[i], transducerTypes[i], dimensions[i],
							minInUnits[i], maxInUnits[i], digitalMin[i], digitalMax[i], prefilterings[i],
							numberOfSamples[i], reserved_area[i], total_samples, filename);

					jsonObject3.put(channelLabels[i].trim(), SignalHeader1);
				}
				w.WriteToHDFS(file2, jsonObject3.toString());
				
				jsonObject3.clear();
				SignalHeader1=null;
				SignalHeader=null;
				result=null;
				bytesindatarecord = NumberOfRecords * bytesineachrecord;
				long blockSize = file.getBlockSize();
				// the unit of block size is byte
				this.numofsplits = (int) Math.ceil((double) bytesindatarecord / blockSize);
				numofrecordsforeachsplit = (int) Math.ceil((double) NumberOfRecords / numofsplits);
				// get the size of each inputsplit
				in.close();
			} catch (Exception e) {
				System.out.println("It's not inputstream");
			}
			// it starts to split behind the header record.
			// goalSize is very closed to blockSize, but it's less than blockSize
			long goalSize = Long.valueOf(numofrecordsforeachsplit * bytesineachrecord);
			long start = bytesinheader;
			long length = file.getLen();
			// get the location information of all blocks containing the entire file
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			// the length of file is not zero and it can be split.
			long blockSize = file.getBlockSize();
			if ((length > blockSize) && isSplitable(job, path)) {
				// calculate how many bytes this file will be split
				long splitSize = Math.max(minSize, Math.min(goalSize, blockSize));
				long bytesRemaining = length;
				// each input split will be obtained by split size
				long size = 0;
				long offset = 0;

				if ((double) (bytesRemaining - start) / splitSize > SPLIT_SLOP) {
					offset = length - bytesRemaining + start;
					blkIndex = getBlockIndex(blkLocations, offset);
					splits.add(new FileSplit(path, offset, splitSize, blkLocations[blkIndex].getHosts()));
					size = start + splitSize;
					bytesRemaining = bytesRemaining - size;

				}
				for (int i = 1; i < numofsplits - 1; i++) {
					if (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
						offset = length - bytesRemaining;
						blkIndex = getBlockIndex(blkLocations, offset);
						splits.add(new FileSplit(path, offset, splitSize, blkLocations[blkIndex].getHosts()));
						bytesRemaining -= splitSize;
					}
				}
				// if the size of the last one is not up to the entire inputsplit, the left data
				// will still be an inputsplit
				if (bytesRemaining != 0) {
					offset = length - bytesRemaining;
					splits.add(new FileSplit(path, offset, bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts()));			
				}
			} else if (length < blockSize) {
				splits.add(new FileSplit(path, start, length - start, new String[0]));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}

		// Returns an array containing all of the elements in this list in proper
		return splits;
	}

	protected boolean isSplitable(JobContext context, Path file) {
		return true;
	}

	@Override

	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new EDFFileRecordReader1();

	}
}
