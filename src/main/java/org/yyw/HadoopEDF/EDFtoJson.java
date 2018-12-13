package org.yyw.HadoopEDF;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;

import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import net.sf.json.JSONObject;



public class EDFtoJson {
	public static void main(String[] args) throws IOException, InterruptedException
	{
		String EDFfilePath = "input1/";
		//String outputPath = "yuanyuan/";
		String outputPath = "/Users/yuanyuan/Desktop/";

		
		File f = new File(EDFfilePath);

		File[] files = f.listFiles(new FilenameFilter() {
			
			public boolean accept(File dir, String name) {
				return name.endsWith(".edf");
			}
		});
		long t1 = System.currentTimeMillis();
		for (int ff = 0; ff<files.length; ff++) {
			File file = files[ff];
			String[] a = file.getAbsolutePath().split("/");
			int len_a = a.length;
			String EDFfilename = a[len_a-1];
			System.out.println("**********************************************************************");
			ToJsonFormat(EDFfilePath,EDFfilename,outputPath);	
		}
		long t2 = System.currentTimeMillis();
		System.out.println("**************" + "Total Time: " + (t2 - t1)/1000.0 + "s" + "**************");		
	}
	
	private static void ToJsonFormat(String filePath,String filename, String outputPath) throws IOException
	{
		
		String edf_name = filename.replace(".edf", "").replaceAll("[^A-Za-z0-9]", "") + ".edf"; // need to change
		String outputFolder = edf_name.replace(".edf", "");
		
		String JsonFilesPath = outputPath + outputFolder;
		File temp = new File(JsonFilesPath);
		temp.mkdir();
		System.out.println(JsonFilesPath + " created");
		
		long t_edf_read = System.currentTimeMillis();
		EDFParserResult result = null;
		File file = new File(filePath+filename);
		InputStream is = null;
		try{
			is = new FileInputStream(file);
			result = EDFParser.parseEDF(is);
		} finally{
			close(is);
		}

		long t_edf_read2 = System.currentTimeMillis();
		System.out.println("EDF load time: " + (t_edf_read2 - t_edf_read)/1000.0 + "s");
		
		EDFHeader header = result.getHeader();
		int channel_no = header.getNumberOfChannels();
		int numberOfRecords=result.getHeader().getNumberOfRecords();
		int numberOfChannels = result.getHeader().getNumberOfChannels();
		JSONObject EDFheader = new JSONObject();
		EDFheader.put("idCode",result.getHeader().getIdCode().trim());
    	EDFheader.put("subjectID",result.getHeader().getSubjectID().trim());
    	EDFheader.put("recordingID",result.getHeader().getRecordingID().trim());
    	EDFheader.put("startDate", result.getHeader().getStartDate().trim());
    	EDFheader.put("startTime", result.getHeader().getStartTime().trim());
    	EDFheader.put("bytesInHeader", result.getHeader().getBytesInHeader());
    	EDFheader.put("formatVersion",result.getHeader().getFormatVersion().trim());
    	EDFheader.put("numberOfRecords",numberOfRecords);
    	EDFheader.put("durationOfRecords",result.getHeader().getDurationOfRecords());
    	EDFheader.put("numberOfChannels",numberOfChannels);
    	
    	String EDFheaserfileName = "EDFheader";		
    	FileWriter EDFheaderFile = new FileWriter(JsonFilesPath + "/EDFheader.json");
    	BufferedWriter outStream = new BufferedWriter(EDFheaderFile);
    	outStream.write(EDFheader.toString());
    	outStream.close();
 
    	JSONObject Signalheader = new JSONObject();
		for(int i_sig = 0; i_sig < channel_no; i_sig++){
			Map<String, Object> SignalInfo = new LinkedHashMap<String, Object>();
			String label = header.getChannelLabels()[i_sig];
			String transducer = header.getTransducerTypes()[i_sig];
			String physical_dimension = header.getDimensions()[i_sig];
			double physical_minimum = header.getMinInUnits()[i_sig];
			double physical_maximum = header.getMaxInUnits()[i_sig];
			double digital_minimum = header.getDigitalMin()[i_sig];
			double digital_maximum = header.getDigitalMax()[i_sig];
			String prefiltering = header.getPrefilterings()[i_sig];
			double samples_per_data_record = header.getNumberOfSamples()[i_sig];
			String reserved_area = String.valueOf(header.getReserveds()[i_sig]);
			int total_samples = result.getSignal().getValuesInUnits()[i_sig].length;
			if(label.trim().equals(""))
				label = "unkwn" +  i_sig;
			if(physical_dimension.trim().equals(""))
				physical_dimension = " ";
//			SignalInfo.put("label", label.trim());
			SignalInfo.put("transducer", transducer.trim());
			SignalInfo.put("physical_dimension", physical_dimension.trim());
			SignalInfo.put("physical_minimum", physical_minimum);
			SignalInfo.put("physical_maximum", physical_maximum);
			SignalInfo.put("digital_minimum", digital_minimum);
			SignalInfo.put("digital_maximum", digital_maximum);
			SignalInfo.put("prefiltering", prefiltering.trim());
			SignalInfo.put("samples_per_data_record", samples_per_data_record);
			SignalInfo.put("reserved_area", reserved_area.trim());
			SignalInfo.put("total_samples", total_samples);
			Signalheader.put(label.trim(), SignalInfo);

			long t_channel = System.currentTimeMillis();
			Integer[] numberOfSample=result.getHeader().getNumberOfSamples();
			double durationofRecords=result.getHeader().getDurationOfRecords();
			double[][] ValuesInUnits = result.getSignal().getValuesInUnits();
		    double[] channelValues =null;
		 
		    	channelValues = ValuesInUnits[i_sig];
		    	int actualnumOfSample = (int) (numberOfSample[i_sig]/durationofRecords);
		    	int len = channelValues.length/actualnumOfSample;

				FileWriter SignalValueFile = new FileWriter(JsonFilesPath + "/" + label.trim() + ".json");
		    	outStream = new BufferedWriter(SignalValueFile); 
		    	int frag_index =0;
		    	for(int i=0;i<len;i++) {
		    		double[] chanelvalueofeachsecond = new double[actualnumOfSample];
		    		  for(int j=0;j<actualnumOfSample;j++) {
		           	     	chanelvalueofeachsecond[j] = channelValues[j+i*actualnumOfSample];
		           		//System.out.println(chanelvalueofeachsecond[j]);
		           		
		       		    }
		    		  //System.out.println(Arrays.toString(chanelvalueofeachsecond));
		    		  frag_index=1+i;
		    		  	if(frag_index==1) {
							outStream.write("{"+"\""+frag_index+"\""+":"+"\""+Arrays.toString(chanelvalueofeachsecond)+"\""+",");
						}else if(frag_index==numberOfRecords) {
							outStream.write("\""+frag_index+"\""+":"+"\""+Arrays.toString(chanelvalueofeachsecond)+"\""+"}");
						}else {
							outStream.write("\""+frag_index+"\""+":"+"\""+Arrays.toString(chanelvalueofeachsecond)+"\""+",");
						}
		    	}
		    	outStream.close();
		    
		}			
		
		FileWriter SignalHeaderFile = new FileWriter(JsonFilesPath + "/SignalHeader.json");
		outStream = new BufferedWriter(SignalHeaderFile);
		outStream.write(Signalheader.toString());
		outStream.close();
		long sysDatetau2 = System.currentTimeMillis();
		System.out.println("**************" + "Data Successfully Uploaded: " + (sysDatetau2 - t_edf_read)/1000.0 + "s" + "**************");		
//		client.close();
	}
	
	private static final void close(Closeable c)
	{
		try
		{
			c.close();
		} catch (Exception e)
		{
			// do nothing
		}
	}

}
