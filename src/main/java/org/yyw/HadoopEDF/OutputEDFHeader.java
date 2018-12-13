package org.yyw.HadoopEDF;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.simple.JSONObject;

public class OutputEDFHeader {
      @SuppressWarnings("unchecked")
	public String OutputEDFHeader(String filename, String idCode, String subjectID,String recordingID,
    		String startDate,String startTime,int bytesInHeader,int numberOfRecords,
            double durationOfRecords,int numberOfChannels) {
    	  JSONObject jsonObject1 = new JSONObject();
    	  Map<String, Object> EDFheader = new LinkedHashMap<String, Object>();
    	    EDFheader.put("idCode",idCode);
	    	EDFheader.put("subjectID",subjectID);
	    	EDFheader.put("recordingID",recordingID);
	    	EDFheader.put("startDate", startDate);
	    	EDFheader.put("startTime", startTime);
	    	EDFheader.put("bytesInHeader", bytesInHeader);
	    	//EDFheader.put("formatVersion",formatVersion);
	    	EDFheader.put("numberOfRecords",numberOfRecords);
	    	EDFheader.put("durationOfRecords",durationOfRecords);
	    	EDFheader.put("numberOfChannels",numberOfChannels);
	    	EDFheader.put("EDFFileName",filename);
	    	jsonObject1.put(filename,EDFheader);
	    	return jsonObject1.toJSONString();
      }
}
