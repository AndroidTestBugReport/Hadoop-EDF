package org.yyw.HadoopEDF;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.simple.JSONObject;

public class OutputSignalHeader {
    public LinkedHashMap<String, Object> OutputSignalHeader(String channelLabels,
            String transducerTypes,String dimensions,Double minInUnits,
            Double maxInUnits,Integer digitalMin,Integer digitalMax,String prefilterings,
            Integer numberOfSamples,byte[] reserved_area,Integer totalsamples,String filename){
    	 //JSONObject jsonObject2 = new JSONObject();
   	     Map<String, Object> Signalheader = new LinkedHashMap<String, Object>();
   	     //Signalheader.put("EDFFileName",filename);
   	   //Signalheader.put("id", filename+"-"+channelLabels.trim());
   	   //Signalheader.put("channelLabel",channelLabels.trim());
   	     Signalheader.put("transducerType",transducerTypes.trim());
   	     Signalheader.put("physical_dimension",dimensions.trim());
   	     Signalheader.put("physical_minimum",minInUnits);
   	     Signalheader.put("physical_maximum",maxInUnits);
   	     Signalheader.put("digital_Minimum",digitalMin);
   	     Signalheader.put("digital_Maximum",digitalMax);
   	     Signalheader.put("prefiltering",prefilterings.trim());
	     Signalheader.put("samples_per_data_record",numberOfSamples);
	     Signalheader.put("reserved_area", String.valueOf(reserved_area));
	     Signalheader.put("total_samples",totalsamples);
	     return (LinkedHashMap<String, Object>) Signalheader;
	   //Signalheader.put("filename", filename);
	     //jsonObject2.putAll(Signalheader);
	     //jsonObject2.put(channelLabels.trim(),Signalheader);
	     //return jsonObject2.toString();
   	     
    }
}
