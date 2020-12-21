package org.yyw.HadoopEDF.ParallelProcessing;

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

import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParser;
import org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram.EDFParserResult;

import net.sf.json.JSONObject;




public class DistributedFile {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// get all file's path in the input folder and parse path to the tool

		// output header information in JsonFormat

		JSONObject jsonObject = new JSONObject();

		String EDFfilePath = "input1/";
		String outputPath = "distributedcache/test1.txt";

		File f = new File(EDFfilePath);
		File[] files = f.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith(".edf");
			}
		});

		try {
			for (File file : files) {
				Map<String, Object> header = new LinkedHashMap<String, Object>();
				String fileName = file.getName();
				String filePath = file.getPath();
				System.out.println(fileName);

				InputStream is = new BufferedInputStream(new FileInputStream(new File(filePath)));
				EDFParserResult result = EDFParser.parseHeader(is);
				int numOfRecords = result.getHeader().getNumberOfRecords();
				header.put("numberOfRecords", numOfRecords);
				header.put("durationOfRecords", result.getHeader().getDurationOfRecords());
				Integer[] numberOfSamples = result.getHeader().getNumberOfSamples();
				header.put("numberOfChannels", result.getHeader().getNumberOfChannels());
				header.put("ChannelLabels", Arrays.toString(result.getHeader().getChannelLabels()).trim());
				header.put("minInUnits", Arrays.toString(result.getHeader().getMinInUnits()).trim());
				header.put("maxInUnits", Arrays.toString(result.getHeader().getMaxInUnits()).trim());
				header.put("digitalMins", Arrays.toString(result.getHeader().getDigitalMin()).trim());
				header.put("digitalMax", Arrays.toString(result.getHeader().getDigitalMax()).trim()); // header.put("prefilterings",
																										// Arrays.toString(result.getHeader().getPrefilterings()).trim());
				header.put("numberOfSamples", Arrays.toString(numberOfSamples).trim());

				jsonObject.put(fileName, header);

				PrintWriter writer = new PrintWriter(outputPath, "UTF-8");
				writer.println(jsonObject.toString());
				writer.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			System.out.println("bingo");
		}
	}
}
