package org.yyw.HadoopEDF;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Arrays;

import java.util.LinkedHashMap;
import java.util.Map;

import net.sf.json.JSONObject;


public class DistributedFile {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//get all file's path in the input folder and parse path to the tool
		
		//output header information in JsonFormat
		
		JSONObject jsonObject = new JSONObject();
		String EDFfilePath = "input";
		//String EDFfilePath = "/Users/yuanyuan/Documents/HadoopEDF/smallfiles";
		String outputPath = "/Users/yuanyuan/Documents/newsmallfile3.txt";
		
		File f = new File(EDFfilePath);
		 //File[] files = f.listFiles();
	    File[] files = f.listFiles(new FilenameFilter(){
	  
	        @Override
	        public boolean accept(File dir, String name){
	          return name.endsWith(".edf");
	        }
	      });
	

		try {
			for(File file : files) {
				    Map<String, Object> header = new LinkedHashMap<String, Object>();
					String fileName = file.getName();
					String filePath = file.getPath() ;
					System.out.println(fileName);
			
					InputStream is = new BufferedInputStream(new FileInputStream(new File(filePath)));
					EDFParserResult result = EDFParser.parseHeader(is);
//					header.put("idCode",result.getHeader().getIdCode().trim());
//					header.put("subjectID",result.getHeader().getSubjectID().trim());
//					header.put("recordingID",result.getHeader().getRecordingID().trim());
//					header.put("startDate", result.getHeader().getStartDate().trim());
//					header.put("startTime", result.getHeader().getStartTime().trim());
//					header.put("bytesInHeader", result.getHeader().getBytesInHeader());
//					header.put("formatVersion",result.getHeader().getFormatVersion().trim());
					int numOfRecords = result.getHeader().getNumberOfRecords();
					header.put("numberOfRecords",numOfRecords);
					header.put("durationOfRecords",result.getHeader().getDurationOfRecords());
					
//				 	Integer sum = 0;
	    	    	Integer[] numberOfSamples = result.getHeader().getNumberOfSamples();
//					for (Integer numofsample : numberOfSamples ) {
//	    	    		 sum = sum + numofsample;   
//	    	    	}
	    	    	//int bytesineachrecord = sum*2 ;
					header.put("numberOfChannels",result.getHeader().getNumberOfChannels());
					
					header.put("ChannelLabels", Arrays.toString(result.getHeader().getChannelLabels()).trim());
					//header.put("transducerTypes", Arrays.toString(result.getHeader().getTransducerTypes()).trim());
					//header.put("dimensions", Arrays.toString(result.getHeader().getDimensions()).trim());
					header.put("minInUnits", Arrays.toString(result.getHeader().getMinInUnits()).trim());
					header.put("maxInUnits", Arrays.toString(result.getHeader().getMaxInUnits()).trim());
					header.put("digitalMins", Arrays.toString(result.getHeader().getDigitalMin()).trim());
					header.put("digitalMax", Arrays.toString(result.getHeader().getDigitalMax()).trim());
					//header.put("prefilterings", Arrays.toString(result.getHeader().getPrefilterings()).trim());
					header.put("numberOfSamples", Arrays.toString(numberOfSamples).trim());
					//header.put("bytesineachrecord", bytesineachrecord);
					//header.put("reserved", result.getHeader().getReserveds());
					
					//System.out.println(header.put("reserved", result.getHeader().getReserveds()));
					jsonObject.put(fileName, header);
					
					PrintWriter writer = new PrintWriter(outputPath, "UTF-8");
					writer.println(jsonObject.toString());
					//writer.println("The second line");
					writer.close();
					
//					FileWriter distributedfile = new FileWriter("/Users/yuanyuanwu/Documents/newfile11.txt");
//					BufferedWriter outStream = new BufferedWriter(distributedfile);
//					outStream.write(jsonObject.toJSONString());
//					outStream.close();
					  // distributedfile.write(jsonObject.toString());
					  //System.out.println(jsonObject.toString());
					
						//distributedfile.write(jsonObject.toJSONString());	
						//distributedfile.close();
					//write header information from all files of the input folder which will be uploaded to HDFS.
									
					//System.out.println(jsonObject);
					
					//System.out.println(fileName);
					//System.out.println("bytesInHeader: " + result.getHeader().getBytesInHeader());		
	}
	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			System.out.println("bingo");
		}	    
   /*
		for(File file : files) {
	FileWriter distributedfile = new FileWriter("/Users/yuanyuan/Documents/distributedfile.txt");
	   distributedfile.write(jsonObject.toJSONString());	
		//distributedfile.write(jsonObject.toJSONString());	
		distributedfile.close();
		
   }*/
	}
}


