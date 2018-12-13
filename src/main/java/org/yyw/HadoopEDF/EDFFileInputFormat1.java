package org.yyw.HadoopEDF;

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

import net.sf.json.JSONObject;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class EDFFileInputFormat1 extends FileInputFormat<Text, BytesWritable> {

	private static final double SPLIT_SLOP = 1; // 10% slop
	// if the following variables should be used for other methods of many files,
	// they have to be defined arraylist
	// I should consider whether define member variables as arraylist, not a single
	// value
	// ArrayList<Integer> numofsplits = new ArrayList();
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
	// public ArrayList<Integer> bytesinheader;
	private Integer NumberOfRecords;
	// public Map<String,Integer> totalNumOfSamples = new HashMap<String,Integer>();
	private Integer NumberOfChannels;
	// private double durationOfRecords;
	// private Double[] minInUnits;
	// private Double[] maxInUnits;
	// private Integer[] digitalMin;
	// private Integer[] digitalMax;
	private Integer[] numberOfSamples;
	private int numofrecordsforeachsplit;
	private Integer bytesineachrecord;
	// public Map<String,Map<String,Integer>> numOfSamplesOfChannel = new
	// HashMap<String,Map<String,Integer>>();
	private int blkIndex;
	// private List<Integer> value = new ArrayList<Integer>();
	// public Map<String,ArrayList<Integer>> SplitId = new
	// HashMap<String,ArrayList<Integer>>();
	// public ArrayList<ArrayList<Integer>> SplitId = new
	// ArrayList<ArrayList<Integer>>();
	private String filename;

	@SuppressWarnings("unchecked")
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		// Path[] inputDir = FileInputFormat.getInputPaths(job);

		List<FileStatus> files = listStatus(job);
		// Save the number of input files in the job-conf
		// job.setLong(NUM_INPUT_FILES, files.length);
		// long totalSize = 0; //compute total size
		// return all files of the input folder, it will throw error if that input
		// folder contains a sub-directory
		// get header information
		for (FileStatus file : files) {
			if (file.isDirectory()) {
				throw new IOException("Not a file: " + file.getPath());
			}
		}

		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>(numofsplits);

		// NetworkTopology clusterMap = new NetworkTopology();
		// split file and obtain each inputsplit's size

		for (FileStatus file : files) {
			// calculate the number of all input files which is 2 now
			Path path = file.getPath();
			filename = path.getName();
			// System.out.println(path);
			Configuration conf = job.getConfiguration();
			FileSystem fs = path.getFileSystem(conf);
			FSDataInputStream in = null;

			Path outputDir = FileOutputFormat.getOutputPath((JobConf) conf);
			// FileSystem fs2 =
			// FileSystem.get(URI.create(outputDir.toString()+"/"+"EDFHeader.txt"), conf);
			//// String file1 = outputDir.toString()+"/"+"EDFHeader.json";
			//// String file2 = outputDir.toString()+"/"+"SignalHeader.json";
			String EDFHeader1 = null;
			// Path inFile = new Path(file1);
			// FSDataOutputStream fileout = fs2.create(inFile);

			// System.out.println(outputDir.getName() + "\n" + outputDir.toString());
			// FSDataOutputStream fileout = inFile.getFileSystem(conf).create(inFile);

			// int bytesinheader;
			try {
				in = fs.open(path);
				// process header record and get the header information
				EDFParserResult result = EDFParser.parseHeader(in);
				// bytesinheader.add(result.getHeader().getBytesInHeader());
				bytesinheader = result.getHeader().getBytesInHeader();
				// System.out.println(filename);
				NumberOfRecords = result.getHeader().getNumberOfRecords();
				// System.out.println(EDFFileInputFormat.getInstance().NumberOfRecords.get(filename));
				NumberOfChannels = result.getHeader().getNumberOfChannels();
				// durationOfRecords = result.getHeader().getDurationOfRecords();
				// channelLabels=result.getHeader().getChannelLabels();
				// minInUnits=result.getHeader().getMinInUnits();
				// maxInUnits=result.getHeader().getMaxInUnits();
				numberOfSamples = result.getHeader().getNumberOfSamples();
				// digitalMin=result.getHeader().getDigitalMin();
				// digitalMax=result.getHeader().getDigitalMax();
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
				// header.put("EDF-Header-id",filename);

				// FileSystem fs2 =
				// FileSystem.get(URI.create(outputDir.toString()+"/"+"EDFHeader.txt"), conf);
				// String file1 = outputDir.toString()+"/"+"EDFHeader"+"-"+filename+".json";
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
				// jsonObject1.put("reserved",reserved);
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

				// Integer[] numberOfSamples1 = result.getHeader().getNumberOfSamples();
				// byte[][] reserveds1 = result.getHeader().getReserveds();
				// System.out.println(channelLabels1[0]);
				OutputSignalHeader SignalHeader = new OutputSignalHeader();

				// String[] SignalHeader1=new String[numberOfChannels1];
				Map<String, Object> SignalHeader1 = new LinkedHashMap<String, Object>();
				// Map<String,Object> y = new LinkedHashMap<String,Object>();
				JSONObject jsonObject3 = new JSONObject();
				Integer total_samples;

				for (int i = 0; i < NumberOfChannels; i++) {
					total_samples = numberOfSamples[i] * NumberOfRecords;
					SignalHeader1 = SignalHeader.OutputSignalHeader(channelLabels[i], transducerTypes[i], dimensions[i],
							minInUnits[i], maxInUnits[i], digitalMin[i], digitalMax[i], prefilterings[i],
							numberOfSamples[i], reserved_area[i], total_samples, filename);
					// System.out.println(SignalHeader1[i]);
					// y.put(channelLabels1[i].trim(), SignalHeader1);
					jsonObject3.put(channelLabels[i].trim(), SignalHeader1);
					// w.WriteToHDFS(file2,jsonObject3.toString());

				}
				/// Signalheader2.put(filename, y);
				// jsonObject3.put(filename, y);
				/// jsonObject3.putAll(Signalheader2);
				w.WriteToHDFS(file2, jsonObject3.toString());
				
				jsonObject3.clear();
				SignalHeader1=null;
				SignalHeader=null;
				result=null;
				
				// System.out.print(bytesineachrecord.get(filename));
				// System.out.print(instance.bytesineachrecord.get(filename));
				// instance.bytesineachrecord = sum *2 ;
				bytesindatarecord = NumberOfRecords * bytesineachrecord;
				// System.out.print(bytesindatarecord);
				// there are two ways to calculate bytesindatarecord
				// bytesindatarecord = (int) (file.getLen() - bytesinheader);
				// calculate the number of map and Encounter a decimal increase to a single
				// digit
				// the default blocksize is 33554432 bytes (32MB)
				long blockSize = file.getBlockSize();
				// System.out.println("blockSize in fileinputformat: "+blockSize);
				// the unit of block size is byte
				this.numofsplits = (int) Math.ceil((double) bytesindatarecord / blockSize);
				// this.numofsplitss = (int) Math.ceil((double)
				// bytesindatarecord/blockSize*1024*1024);
				// the number of records in each block or in each inputsplit
				// the targetsize can be obtained by adding filesize and the number of splits
				// times bytesinheader which is divided by numofsplitss
				// long targetsize = (file.getLen()+(numofsplits-1)*bytesinheader)/numofsplitss
				// == 0 ? 1 : numofsplits;
				// long targetsize = bytesindatarecord/numofsplitss;
				// int numofrecords = targetsize/numofsplitss; 注意targetsize is long type
				numofrecordsforeachsplit = (int) Math.ceil((double) NumberOfRecords / numofsplits);
				// System.out.println("numofrecordsforeachsplit in file inputformat:
				// "+numofrecordsforeachsplit);
				// numofrecordsforeachsplit =(int) Math.floor((double)
				// blockSize/instance.bytesineachrecord.get(filename));
				// the substitute method is numofrecordsforeachsplit =(int) Math.ceil((double)
				// blockSize/bytesineachrecord);
				// this computation is obtained by average spliting.numofrecordsforeachsplit
				// =(int) Math.ceil((double) NumberOfRecords/numofsplitss);
				// 由于我遇到小数就加1，问题就是最后一个如果不满这个size的话是要加0，还是只保留实际剩余的值，I prefer the second way.
				// 如果舍弃小数，那么加在一起达不到number of records 多余的添加到最后一个block里，问题是担心超过一个block size.
				// get the size of each inputsplit
				in.close();
			} catch (Exception e) {
				System.out.println("It's not inputstream");
			}
			// totalSize += file.getLen();
			// it starts to split behind the header record.
			// goalSize is very closed to blockSize, but it's less than blockSize
			long goalSize = Long.valueOf(numofrecordsforeachsplit * bytesineachrecord);
			// System.out.println(goalSize);
			long start = bytesinheader;
			long length = file.getLen();
			// get the location information of all blocks containing the entire file
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			// the length of file is not zero and it can be split.
			long blockSize = file.getBlockSize();
			if ((length > blockSize) && isSplitable(job, path)) {
				// calculate how many bytes this file will be split
				long splitSize = Math.max(minSize, Math.min(goalSize, blockSize));
				// System.out.println("splitSize: "+splitSize);
				// the substitute method is long splitSize = Math.max(minSize,
				// Math.max(goalSize, blockSize));
				// it will start to split behind bytesinheader
				// long bytesRemaining = length - bytesinheader;
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
					// SplitId[numofsplits-1] = numofsplits-1;
					// value.add(numofsplits-1);
					// System.out.println(value.add(numofsplits-1));
					// SplitId.get(x).add(numofsplits-1);
				}
			} else if (length < blockSize) {
				//???????if the file's size is less than the blocksize, I still split it
				splits.add(new FileSplit(path, start, length - start, new String[0]));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
			// instance.SplitId.put(filename, (ArrayList<Integer>) value);

			// LOG.debug("Total # of splits" + splits.size());
		}

		// Returns an array containing all of the elements in this list in proper
		// sequence (from first to last element);
		//System.gc();
		// Runtime.getRuntime().gc();
		return splits;

		// return splits.toArray(new FileSplit[splits.size()]);

	}

	protected boolean isSplitable(JobContext context, Path file) {
		return true;
	}

	@Override

	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// segment++;
		return new EDFFileRecordReader1();

	}
}
