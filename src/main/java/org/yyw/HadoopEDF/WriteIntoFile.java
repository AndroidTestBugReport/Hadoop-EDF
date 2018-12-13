package org.yyw.HadoopEDF;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

public class WriteIntoFile {
	//private static final String FILENAME = "/Users/yuanyuan/Converted version of EDF file/chat-baseline-300001.txt";
	private static final String FILENAME="/Users/yuanyuan/Desktop/shhs1-200002.txt";
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		
		String pathToEdfFile = "input/shhs1-200002.edf";
		InputStream is = new BufferedInputStream(new FileInputStream(new File(pathToEdfFile)));
		EDFParserResult result = EDFParser.parseEDF(is);
		
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(FILENAME))){
			bw.write(result.getHeader().getIdCode() + "\n" );
			bw.write(result.getHeader().getSubjectID()+ "\n");
			bw.write(result.getHeader().getRecordingID() + "\r\n");
			bw.write(result.getHeader().getStartDate()+ "\r\n");
			bw.write(result.getHeader().getStartTime()+ "\r\n");
			bw.write((int)result.getHeader().getBytesInHeader() + "\r\n");
			bw.write(result.getHeader().getFormatVersion()+ "\r\n");
			bw.write(result.getHeader().getNumberOfRecords()+ "\r\n");
			bw.write((int) result.getHeader().getDurationOfRecords()+ "\r\n");
			bw.write((int) result.getHeader().getNumberOfChannels()+ "\r\n");
			
		   
			String[] ChannelLabels=result.getHeader().getChannelLabels();
			for(int i=0; i<ChannelLabels.length; i++){
				bw.write(ChannelLabels[i]+" ");
			}
			bw.newLine();
			String[] transducerTypes=result.getHeader().getTransducerTypes();
			for(int i=0; i<transducerTypes.length; i++){
				bw.write(transducerTypes[i]+" " );
			}
			
			String[] dimensions=result.getHeader().getDimensions();
			for(int i=0; i<dimensions.length; i++){
				bw.write(dimensions[i] +" ");
			}
			
			Double[] minInUnits=result.getHeader().getMinInUnits();
			for(int i=0; i<minInUnits.length; i++){
				bw.write(minInUnits[i] +" ");
			}
			
			Double[] maxInUnits=result.getHeader().getMaxInUnits();
			for(int i=0; i<maxInUnits.length; i++){
				bw.write(maxInUnits[i] +" ");
			}
			
			Integer[] digitalMin=result.getHeader().getDigitalMin();
			for(int i=0; i<digitalMin.length;i++){
				bw.write(digitalMin[i] +" ");
			}
			
			Integer[] digitalMax=result.getHeader().getDigitalMax();
			for(int i=0; i<digitalMax.length;i++){
				bw.write(digitalMax[i] +" ");
			} 
			
			String[] prefilterings=result.getHeader().getPrefilterings();
			for(int i=0; i<prefilterings.length; i++){
				bw.write(prefilterings[i]+" ");
			}

			Integer[] numberOfSamples=result.getHeader().getNumberOfSamples();
			for(int i=0; i<numberOfSamples.length;i++){
				bw.write(numberOfSamples[i]+" "+"\n" );
				
			}
			/*byte[][] reserveds=result.getHeader().getReserveds();
			for(int i=0; i<reserveds.length;i++){
				for(int j=0; j<reserveds[i].length;j++){
				bw.write(reserveds[i][j]+" ");
				}
			}*/
		   double[][] valuesInUnits=result.getSignal().getValuesInUnits();
		   for(int i=0; i<1;i++){
		    	for(int j=0;j<valuesInUnits[i].length;j++){
				bw.write(valuesInUnits[i][j]+" ");	
				//if(i==11){
				//	 bw.write(valuesInUnits[i][j]+" ");	
				//}
				
			System.out.println("Done");}
		   }
			}catch (IOException e){
			e.printStackTrace();
		}	
	}
}
