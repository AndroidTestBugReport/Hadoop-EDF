package org.yyw.HadoopEDF;
import java.io.BufferedReader;
import java.io.File;

import java.io.FileReader;
import java.io.IOException;

import net.sf.json.JSONObject;

public class test1 {

	@SuppressWarnings("null")
	public static void main(String[] args)throws Exception{
//		String idCode = null;
//        String subjectID = null;
//        String recordingID = null;
//        String startDate = null;
//        String startTime = null;
//        int bytesInHeader = 0;
//        String formatVersion = null;
//        int numberOfRecords = 0;
//        double durationOfRecords = 0;
        int numberOfChannels = 0;
        int numOfRecords = 0;
        String[] channelLabels = null;
        String[] transducerTypes = null;
        String[] dimensions = null;
        Double[] minInUnits = null;
        Double[] maxInUnits = null;
        Integer[] digitalMin = null;
        Integer[] digitalMax = null;
        String[] prefilterings = null;
        Integer[] numberOfSamples = null;
    	try {	
    		test1 a = new test1();
    		//get the contents of Json file
    		String b =a.ReadFile("/Users/yuanyuan/Documents/file12.txt");
    		//System.out.println(b);
    	    String filename = "shhs1-200001.edf";
			JSONObject j1 = JSONObject.fromObject(b).getJSONObject(filename );
//    		idCode = (String) j1.get("idCode");
//    		subjectID = j1.getString("subjectID");
//    		recordingID = j1.getString("recordingID");
//    		startDate=j1.getString("startDate");
//    		startTime =j1.getString("startTime");
//    		bytesInHeader = (Integer) j1.get("bytesInHeader");
//    		numberOfRecords =(Integer) j1.get("numberOfRecords");
//    		durationOfRecords = j1.getDouble("durationOfRecords");
			numOfRecords=j1.getInt("numberOfRecords");
			System.out.println(numOfRecords);
    		numberOfChannels = j1.getInt("numberOfChannels");
    		Object bb = j1.get("ChannelLabels");
    		//System.out.println(j1.get("ChannelLabels"));
    	
    		channelLabels =j1.get("ChannelLabels").toString().replace("[", "").replace("]", "").replace(" ", "").split(",");
//    		transducerTypes=j1.get("transducerTypes").toString().replace("[", "").replace("]", "").split(",");
//    	    dimensions = j1.get("dimensions").toString().replace("[", "").replace("]", "").split(",");
    	    String[] mi = j1.get("minInUnits").toString().replace("[", "").replace("]", "").split(",");
    	    minInUnits = new Double[mi.length];
    	    for(int i = 0;i<mi.length;i++) {
    	    	minInUnits[i] = Double.parseDouble(mi[i]);
    	    	//System.out.println(minInUnits[i]);
    	    }
    	    String[] ma = j1.get("maxInUnits").toString().replace("[", "").replace("]", "").split(",");
    	    maxInUnits = new Double[ma.length];
    	    for(int i = 0;i<ma.length;i++) {
    	    	maxInUnits[i] = Double.parseDouble(ma[i]);
    	    	//System.out.println(maxInUnits[i]);
    	    }
    	   
    	    String dmi1 = j1.get("digitalMins").toString().replace("[", "").replace("]", "").replace(" ", "");
    	    System.out.println(dmi1);
    	    String[] dmi = dmi1.substring(0).split(",");
    	    digitalMin = new Integer[dmi.length];
    	    for(int i = 0;i<dmi.length;i++) {
    	    	digitalMin[i] = Integer.parseInt(dmi[i]);
    	    	//System.out.println(digitalMin[i]);
    	    }
    	    String dma1 = j1.get("digitalMax").toString().replace("[", "").replace("]", "").replace(" ", "");
    	    String[] dma = dma1.substring(0).split(",");
    	    digitalMax = new Integer[dma.length];
    	    for(int i = 0;i<dma.length;i++) {
    	    	digitalMax[i] = Integer.parseInt(dma[i]);
    	    	//System.out.println(digitalMax[i]);
    	    }
    	   // prefilterings=j1.get("prefilterings").toString().replace("[", "").replace("]", "").split(","); 
    	    
    	    String nn = j1.get("numberOfSamples").toString().replace("[", "").replace("]", "").replace(" ", "");
    	    String[] ns = nn.substring(0).split(",");
    	    numberOfSamples = new Integer[ns.length];
    	    for(int i = 0;i<ns.length;i++) {
    	    	numberOfSamples[i] = Integer.parseInt(ns[i]);
    	    	//System.out.println(numberOfSamples[i]);
    	    }
    				
     	}finally {
    		System.out.println("bingo");
    	}
    		//System.out.println(idCode);
    		//System.out.println(numberOfRecords);
    		//System.out.println(numberOfChannels);
    	
   
    	
    	
    }
	// 读文件，返回字符串
    public String ReadFile(String path) {
        File file = new File(path);
        BufferedReader reader = null;
        String laststr = "";
        try {
            // System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
               // System.out.println(tempString);
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
        return laststr;
    }
}
