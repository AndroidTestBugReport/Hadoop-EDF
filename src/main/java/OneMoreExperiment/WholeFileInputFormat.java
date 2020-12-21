package OneMoreExperiment;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.yyw.HadoopEDF.ParallelProcessing.OutputSignalHeader;
import org.yyw.HadoopEDF.ParallelProcessing.WritetoHDFS;
import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParser;
import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParserResult;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class WholeFileInputFormat extends FileInputFormat<Text, FSDataInputStream> {
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
	//private int numofrecordsforeachsplit;
	private Integer bytesineachrecord;
	//private int blkIndex;
	private String filename;

	@Override
    protected boolean isSplitable(JobContext job, Path path) {
    	List<FileStatus> files = null;
		try {
			files = listStatus(job);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	//for (FileStatus file : files) {
			// calculate the number of all input files which is 2 now
			//Path path2 = file.getPath();
			//System.out.println(path);
			//System.out.println(path2);
			filename = path.getName();
			System.out.println(filename);
			Configuration conf = job.getConfiguration();
			FileSystem fs = null;
			try {
				fs = path.getFileSystem(conf);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			FSDataInputStream in = null;

			Path outputDir = FileOutputFormat.getOutputPath((JobConf) conf);
		
	
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
                //System.out.println("NumberOfRecords :"+NumberOfRecords );
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
				//System.out.print(jsonObject3);
				w.WriteToHDFS(file2, jsonObject3.toString());
			
				jsonObject3.clear();
				SignalHeader1=null;
				SignalHeader=null;
				result=null;
				in.close();
		 }catch (Exception e) {
				System.out.println("It's not inputstream");
			}
    	//}
        return false;
    }

	@Override
	public RecordReader<Text, FSDataInputStream> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new WholeFileRecordReader();
	}
}
// public class WholeFileInputFormat {
// protected boolean isSplitable(JobContext context, Path filename) {
// return false;
// }
//
// public WholeFileRecordReader createRecordReader(
// InputSplit split, TaskAttemptContext context) {
// return new WholeFileRecordReader();
// }
// }
