package org.yyw.HadoopEDF.ParallelProcessing.ComparingResults;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class CompareSingleLargefileWithSplitting {

	public static void main(String[] args) throws IOException{
		
		String bucketName = "yuanyuan-edf";
		
		String parallel=args[0]+"/"+"chat-baseline-300033.edf"+"/";
		String sequential=args[1]+"/"+"10-2/"+"chatbaseline300033"+"/";
		
		System.out.println(parallel);
		System.out.println(sequential);
		
		List<String> fileNameList = new ArrayList<String>();
		GetObjectRequest request=null;
		S3Object object=null;
		
		List<String> fileNameList2 = new ArrayList<String>();
		GetObjectRequest request2=null;
		S3Object object2=null;
		
		try {
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
		
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(parallel).withDelimiter("/");
		ListObjectsV2Result objects = s3Client.listObjectsV2(req);
		
		AmazonS3 s3Client2 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
		
		ListObjectsV2Request req2 = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(sequential).withDelimiter("/");
		ListObjectsV2Result objects2 = s3Client.listObjectsV2(req2);
		
		for (S3ObjectSummary object1 : objects.getObjectSummaries()) {
			if(object1.getKey().contains("r")) {
				fileNameList.add(object1.getKey());
				 System.out.println("getting filename by getKey() from parallel: "+object1.getKey());
			}
		}
		
		for (S3ObjectSummary object21 : objects2.getObjectSummaries()) {
			if(object21.getKey().contains(".json")) {
				fileNameList2.add(object21.getKey());
				 System.out.println("getting filename by getKey() from sequential: "+object21.getKey());
			}
		}
		 InputStream is1=null;
		 InputStream is2=null;
		 
		 String in1 = null;
		 String in2=null;
		 int index=0;
		for(int f=0;f<fileNameList.size()-1;f++) {
			index+=1;
			request = new GetObjectRequest(bucketName,fileNameList.get(f));	
			object = s3Client.getObject(request);
	        is1 = object.getObjectContent();  
	        
	        request2 = new GetObjectRequest(bucketName,fileNameList2.get(f));
			object2 = s3Client2.getObject(request2);
			 is2 = object2.getObjectContent();
			 
			 boolean compare1and2= isEqual(is1,is2);
			 System.out.println("the file list of sequential"+","+fileNameList2.get(f));
			 System.out.println("the file list of parallel"+","+fileNameList.get(f));
			 System.out.println("compare parallel and sequential: "+compare1and2);
			 is1.close();
			 is2.close();

		 }
	
	          //index is the last index of the element in fileNameList
	      	  if(fileNameList.get(index).contains("DHR")) {
	      		  request = new GetObjectRequest(bucketName,fileNameList.get(index));	
				  object = s3Client.getObject(request);
		          is1 = object.getObjectContent();  
				  in1=in1+getStringFromInputStream(is1);  
				  is1.close();
				}
	      	  int n = fileNameList.size()-index;
	      	  for(int j=0;j<n;j++) {
	      		  request2 = new GetObjectRequest(bucketName,fileNameList2.get(index+j));
				  object2 = s3Client2.getObject(request2);
				  is2 = object2.getObjectContent();
	      		  in2=in2+getStringFromInputStream(is2);
	      		  is2.close();
	      	  }
	      	  System.out.println("the length of in1"+","+in1.length());
			  System.out.println("the length of in2"+","+in2.length());
			  if(in1.equals(in2)) {
					System.out.println("compare DHR of parallel and sequential: "+true);
				}else {
					System.out.println("compare DHR of parallel and sequential: "+false);
				}

	}catch(AmazonServiceException e) {
		e.printStackTrace();
	}catch(SdkClientException e) {
		e.printStackTrace();
	}
	}

	private static boolean isEqual(InputStream is1, InputStream is2) throws IOException{
		// TODO Auto-generated method stub
		String in1 = getStringFromInputStream(is1);
		String in2 = getStringFromInputStream(is2);
		boolean result=in1.equals(in2);
		return result;
	}
	private static String getStringFromInputStream(InputStream is) {
		// TODO Auto-generated method stub
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		String line;
		try {

			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return sb.toString();
	}
}
