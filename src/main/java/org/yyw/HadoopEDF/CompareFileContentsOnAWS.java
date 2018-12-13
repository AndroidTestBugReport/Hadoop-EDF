package org.yyw.HadoopEDF;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;

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

public class CompareFileContentsOnAWS {

	public static void main(String[] args) throws IOException{
		
		String bucketName = "yuanyuan-edf";
		
		String parallel=args[0]+"/"+"shhs2-200077.edf"+"/";
		String sequential=args[1]+"/"+"shhs2200077"+"/";
		
		System.out.println(parallel);
		System.out.println(sequential);
		
		List<String> fileNameList = new ArrayList<String>();
		GetObjectRequest request=null;
		S3Object object=null;
		try {
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
		
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(parallel).withDelimiter("/");
		ListObjectsV2Result objects = s3Client.listObjectsV2(req);
		
		for (S3ObjectSummary object1 : objects.getObjectSummaries()) {
			if(object1.getKey().contains("r")) {
				fileNameList.add(object1.getKey());
				 System.out.println("getting filename by getKey() from parallel: "+object1.getKey());
			}
		}
		
		for(String file : fileNameList) {
			
			//GetObjectRequest request = new GetObjectRequest(bucketName,file);
			request = new GetObjectRequest(bucketName,file);
	        //S3Object object = s3Client.getObject(request);
			object = s3Client.getObject(request);
	        InputStream is = object.getObjectContent();
	        System.out.println(file);
	        //EDFParserResult result = EDFParser.parseEDF(is);
	       
	        
	}
		
		List<String> fileNameList2 = new ArrayList<String>();
		GetObjectRequest request2=null;
		S3Object object2=null;
	
		AmazonS3 s3Client2 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
		
		ListObjectsV2Request req2 = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(sequential).withDelimiter("/");
		ListObjectsV2Result objects2 = s3Client.listObjectsV2(req2);
		
		for (S3ObjectSummary object21 : objects2.getObjectSummaries()) {
			if(object21.getKey().contains(".json")) {
				fileNameList.add(object21.getKey());
				 System.out.println("getting filename by getKey() from sequential: "+object21.getKey());
			}
		}
		
		for(String file : fileNameList2) {
			
			//GetObjectRequest request = new GetObjectRequest(bucketName,file);
			request = new GetObjectRequest(bucketName,file);
	        //S3Object object = s3Client.getObject(request);
			object2 = s3Client2.getObject(request);
	        InputStream is = object2.getObjectContent();
	        System.out.println(file);
	        //EDFParserResult result = EDFParser.parseEDF(is);      
	        }
		
//		
//		File dir1 = new File(parallel);
//		File dir2 = new File(sequential);
//		
//		getDiff(dir1,dir2);
//	
	}catch(AmazonServiceException e) {
		e.printStackTrace();
	}catch(SdkClientException e) {
		e.printStackTrace();
	}
	}
		

//
//	private static void getDiff(File dir1, File dir2) throws IOException{
//		// TODO Auto-generated method stub
//		// scan the first level of folder
//		File[] fileList1 = dir1.listFiles();
//		File[] fileList2 = dir2.listFiles();
//		for(int i=0;i<fileList1.length;i++) {
//			//scan the subfolder of that level
//			File[] subfileList1 = null;
//			File[] subfileList2 = null;
//			
//			if(fileList1[i].isDirectory() && fileList2[i].isDirectory() ) {
//				subfileList1 = fileList1[i].listFiles();
//				subfileList2 = fileList2[i].listFiles();
//			}
//			for(int j=0;j<subfileList1.length;j++) {
//				//a and b are the path as well
//				String a =subfileList1[i].toString();
//				String b =subfileList2[i].toString();
//				//pay attention the difference of name between parallel and sequential
//				String[] testparallel=a.split("/");
//				String[] testsequential=b.split("/");
//				String edf_name1=testparallel[2];
//				String edf_name2=testsequential[2];
//				
//				String[] test1=edf_name1.split("-");
//                //name1 and name2 are the name
//				String name1=test1[0]+".json";
//				String name2 =edf_name2;
//				System.out.println(name1);
//				System.out.println(name2);
//                if(name1.equals(name2)) {
//                    System.out.println("subfile's name is same");
//                    File file1 = new File(a);
//                    File file2 = new File(b);
//                    boolean compare1and2=FileUtils.contentEquals(file1, file2);
//                    System.out.println("Are "+a+" and "+b+" the same? "+ compare1and2);
//			    }else {
//			    	System.out.println("subfile's name is not same: "+"false");
//			
//		        }
//                
//		
//	}
//	}
//	}
	}

//if(args.length !=2) throw (new RuntimeException("Usage: java FileCompare parallel sequential"));{
//    String parallel=args[0];
//    String sequential=args[1];
    
//File file1 = new File("file1.txt");
//File file2 = new File("file2.txt");
//boolean compare1and2=FileUtils.contentEquals(file1, file2);
//System.out.println("Are test1.txt and test2.txt the same? "+ compare1and2);
//}
