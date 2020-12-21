package org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram;


import java.io.ByteArrayInputStream;
import java.io.Closeable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

import net.sf.json.JSONObject;
//import yuanyuan11.EDFHeader;
//import yuanyuan11.EDFParser;
//import yuanyuan11.EDFParserResult;

public class EDFtoJsonyuanyuanforsmallfiles {
	public static void main(String[] args) throws IOException, InterruptedException
	{
		String bucketName = "yuanyuan-edf";
		String EDFfilePath = args[0]+"/";
		String outputPath="Seq"+args[0]+"/";

        
		List<String> fileNameList = new ArrayList<String>();
		
		//AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
		//		.withCredentials(new InstanceProfileCredentialsProvider(false)).build();
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
		
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(EDFfilePath).withDelimiter("/");
		ListObjectsV2Result objects = s3Client.listObjectsV2(req);
		
		for (S3ObjectSummary object : objects.getObjectSummaries()) {
			if(object.getKey().contains("edf")) {
				fileNameList.add(object.getKey());
				 System.out.println("getting filename by getKey(): "+object.getKey());
			}
		}
		long t1 = System.currentTimeMillis();
		for(String file : fileNameList) {
			
			GetObjectRequest request = new GetObjectRequest(bucketName,file);
	        S3Object object = s3Client.getObject(request);
	        InputStream is = object.getObjectContent();
	        System.out.println(file);
	        EDFParserResult result = EDFParser.parseEDF(is);
	      
	        System.out.println("---------there is no error for java heap space with the EDF Parser result-----");
	        is.close();
	     
			    long t_edf_read = System.currentTimeMillis();
        	   
        	    String edf_name = file.replace(EDFfilePath, "").replace(".edf", "").replaceAll("[^A-Za-z0-9]", "") + ".edf";
    	    	String outputFolder = edf_name.replace(".edf", "");
 	
    			String JsonFilesPath = outputPath + outputFolder;

    			System.out.println(JsonFilesPath + " created");
    		

			EDFHeader header = result.getHeader();
			int channel_no = header.getNumberOfChannels();
			int numberOfRecords=result.getHeader().getNumberOfRecords();
			JSONObject EDFheader = new JSONObject();
			EDFheader.put("idCode",result.getHeader().getIdCode().trim());
	    	EDFheader.put("subjectID",result.getHeader().getSubjectID().trim());
	    	EDFheader.put("recordingID",result.getHeader().getRecordingID().trim());
	    	EDFheader.put("startDate", result.getHeader().getStartDate().trim());
	    	EDFheader.put("startTime", result.getHeader().getStartTime().trim());
	    	EDFheader.put("bytesInHeader", result.getHeader().getBytesInHeader());
	    	EDFheader.put("formatVersion",result.getHeader().getFormatVersion().trim());
	    	EDFheader.put("numberOfRecords",result.getHeader().getNumberOfRecords());
	    	EDFheader.put("durationOfRecords",result.getHeader().getDurationOfRecords());
	    	EDFheader.put("numberOfChannels",result.getHeader().getNumberOfChannels());
	    	
	    	String EDFheaserfileName = "EDFheader";		
	   
	    	String rPathheader = JsonFilesPath+ "/EDFheader.json";
	    	System.out.println(rPathheader);
	    	s3Client.putObject(bucketName,rPathheader,EDFheader.toString());
	    	
	    	EDFheader.clear();
	    	EDFheader=null;
	    	
	    	JSONObject Signalheader = new JSONObject();
	    	double[][] valuesInUnits= new double[channel_no][] ;
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
//				SignalInfo.put("label", label.trim());
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
				int actualNumofSamples = (int) (header.getNumberOfSamples()[i_sig]/header.getDurationOfRecords());
				int sig_pre_loc = 0;
				int frag_index = 0;
				int sig_loc = 1*actualNumofSamples - 1;
		
			     Double[] maxInUnits = result.getHeader().getMaxInUnits();
			     Double[] minInUnits = result.getHeader().getMinInUnits();
			     Integer[] digitalMin = result.getHeader().getDigitalMin();
			     Integer[] digitalMax = result.getHeader().getDigitalMax();
			     
			      Integer[] numberofsamples =header.getNumberOfSamples();
			  
			      System.out.println("bingo");
			                
				valuesInUnits=result.getSignal().getValuesInUnits();
				
                JSONObject SignalValue = new JSONObject();
//			    String rPathSignalValueFile = JsonFilesPath+ "/" + label.trim() + ".json";
//			    System.out.println(rPathSignalValueFile);
//			    
//			    String ValuesOfEachChannel="";
				int signal_len = valuesInUnits[i_sig].length;
				while (sig_loc < signal_len){
					frag_index = frag_index + 1;
					ArrayList<Double> fragment = new ArrayList<Double>();
					for(int iv = sig_pre_loc; iv<= sig_loc; iv++){
						fragment.add(valuesInUnits[i_sig][iv]);
					}
//					String SignalValue = "";
//					if(frag_index==1) {
//						SignalValue="{"+"\""+frag_index+"\""+":"+"\""+fragment.toString()+"\""+",";
//						//outStream.write("{"+"\""+frag_index+"\""+":"+"\""+fragment.toString()+"\""+",");
//					}else if(frag_index==numberOfRecords) {
//						SignalValue="\""+frag_index+"\""+":"+"\""+fragment.toString()+"\""+"}";
//						//outStream.write("\""+frag_index+"\""+":"+"\""+fragment.toString()+"\""+"}");
//					}else {
//						SignalValue="\""+frag_index+"\""+":"+"\""+fragment.toString()+"\""+",";
//						//outStream.write("\""+frag_index+"\""+":"+"\""+fragment.toString()+"\""+",");
//					}
//					ValuesOfEachChannel=ValuesOfEachChannel+SignalValue;
					
					SignalValue.put(frag_index, "\""+fragment.toString()+"\"");
	               
					sig_pre_loc = sig_loc + 1;
					sig_loc = sig_loc + 1*actualNumofSamples;
				}
				
       		      String rPathSignalValueFile = JsonFilesPath+ "/" + label.trim() + ".json";
			      System.out.println(rPathSignalValueFile);
			      String values=SignalValue.toString();
			    
			      SignalValue.clear();
			      SignalValue=null;
			    s3Client.putObject(bucketName, rPathSignalValueFile, values);
//			    ObjectMetadata meta = new ObjectMetadata();
//			    meta.setContentLength(values.length());
//			    System.out.println(values.length());
//			    s3Client.putObject(bucketName,rPathSignalValueFile,new ByteArrayInputStream(values.getBytes()),meta);
			    
		}
		is.close();
	 	String rPathSignalHeader = JsonFilesPath+ "/SignalHeader.json";
	 	//System.out.println(rPathSignalHeader);
    	s3Client.putObject(bucketName,rPathSignalHeader,Signalheader.toString());
    	
    	Signalheader.clear();
    	Signalheader=null;
    	
		long sysDatetau2 = System.currentTimeMillis();
		System.out.println("**************" + "Data Successfully Uploaded: " + (sysDatetau2 - t_edf_read)/1000.0 + "s" + "**************");	
        }
			long t2 = System.currentTimeMillis();
			System.out.println("**************" + "Total Time: " + (t2 - t1)/1000.0 + "s" + "**************");	
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

