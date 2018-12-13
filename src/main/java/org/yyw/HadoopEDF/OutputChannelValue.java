package org.yyw.HadoopEDF;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;

public class OutputChannelValue {

	public LinkedHashMap<String,Object> OutputChannelValue(int index, String chanelvalueofeachsecond){
    	 //JSONObject jsonObject2 = new JSONObject();
   	     Map<String, Object> SignalValue = new LinkedHashMap<String, Object>();
   	     //Signalheader.put("EDFFileName",filename);
   	   //Signalheader.put("id", filename+"-"+channelLabels.trim());
   	   
   	     SignalValue.put("fragment_index",index);
   	     SignalValue.put("fragment",chanelvalueofeachsecond);
   	     
	     return (LinkedHashMap<String,Object>) SignalValue;
}
}