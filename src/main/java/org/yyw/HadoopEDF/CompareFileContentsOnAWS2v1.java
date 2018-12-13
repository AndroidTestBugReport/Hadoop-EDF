package org.yyw.HadoopEDF;

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

public class CompareFileContentsOnAWS2v1 {

	public static void main(String[] args) throws IOException{

		String bucketName = "yuanyuan-edf";
		
		for(int i=200077;i<200335;i++) {
			String parallel=args[0]+"/"+"shhs2-"+i+".edf"+"/";
			String sequential=args[1]+"/"+"shhs2"+i+"/";	
			//		String parallel=args[0]+"/"+"shhs2-200078.edf"+"/";
			//		String sequential=args[1]+"/"+"shhs2200078"+"/";

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
				for(int f=0;f<fileNameList.size();f++) {

					//GetObjectRequest request = new GetObjectRequest(bucketName,file);
					request = new GetObjectRequest(bucketName,fileNameList.get(f));
					request2 = new GetObjectRequest(bucketName,fileNameList2.get(f));
					//S3Object object = s3Client.getObject(request);
					object = s3Client.getObject(request);
					object2 = s3Client2.getObject(request2);

					is1 = object.getObjectContent();
					is2 = object2.getObjectContent();

					boolean compare1and2= isEqual(is1,is2);
					System.out.println("the file list of parallel"+","+fileNameList.get(f));
					System.out.println("the file list of sequential"+","+fileNameList2.get(f));
					System.out.println("compare parallel and sequential: "+compare1and2);
					is1.close();
					is2.close();
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
	}

		private static boolean isEqual(InputStream is1, InputStream is2) throws IOException{
			// TODO Auto-generated method stub
			//		String in1 = new BufferedReader(new InputStreamReader(is1)).readLine(); 
			//		String in2 = new BufferedReader(new InputStreamReader(is2)).readLine(); 
			//		
			//		System.out.println(in1);
			//		System.out.println(in2);
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
	//	public static String convert(InputStream inputStream, Charset charset) throws IOException {
	//		 
	//		StringBuilder stringBuilder = new StringBuilder();
	//		String line = null;
	//		
	//		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, charset))) {	
	//			while ((line = bufferedReader.readLine()) != null) {
	//				stringBuilder.append(line);
	//			}
	//		}
	//	 
	//		return stringBuilder.toString();
	//	}


	//
	//	          byte[] buffer1 = new byte[1024];
	//	          byte[] buffer2 = new byte[1024];
	//	    
	//	          
	//	              int numRead1 = 0;
	//	              int numRead2 = 0;
	//	              int m=0;
	//	              int n=0;
	//	              while (true) {
	//	                  numRead1 = input1.read(buffer1);
	//	                  numRead2 = input2.read(buffer2);
	//	                    	  if (!Arrays.equals(buffer1, buffer2)) {
	//	                    		  n++;
	//	                    		  return false;
	//	                    	  }else {
	//	                    		// Otherwise same bytes read, so continue ...
	//	                    		  m++;
	//	                    		  return true;
	//	                    	  }	 
	//	                                    
	//	              
	//	              }
	////	           	System.out.println("the number of times for correctness: "+m);
	////	           	System.out.println("the number of times for incorrectness: "+n);
	//	              }
	//	}


	//	   	int len;
	//     	int pos = 0;
	//     	byte[] buffer = new byte[1];
	//     	int size = 1024;
	//     	byte[] data = new byte[size];
	//     	
	//	   	int len2;
	//     	int pos2 = 0;
	//     	byte[] buffer2 = new byte[1];
	//     	int size2 = 1024;
	//     	byte[] data2 = new byte[size2];
	//     	int m=0;
	//     	int n=0;
	//     	while(pos < size && (len = is1.read(buffer)) != -1 && pos2 < size2 && (len2 = is2.read(buffer2)) != -1) {
	//     		data[pos] = buffer[0];
	//     		System.out.println(data.length);
	//     		pos++;
	//     		
	//     		data2[pos2] = buffer2[0];
	//     		System.out.println(data2.length);
	//     		pos2++;
	//     		
	//     		if (Arrays.equals(data, data2)) {
	//     			m++;
	//     		
	//     		}else {
	//     			n++;
	//     		
	//     		}
	//     	}
	//		//return false;
	//     	System.out.println("the number of times for correctness: "+m);
	//     	System.out.println("the number of times for incorrectness: "+n);
	//     	if(m>0) {
	//
	//		return true;
	//
	//	    }else {
	//        return false;
	//	    }
	//	}
	//}



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


	//if(args.length !=2) throw (new RuntimeException("Usage: java FileCompare parallel sequential"));{
	//    String parallel=args[0];
	//    String sequential=args[1];

	//File file1 = new File("file1.txt");
	//File file2 = new File("file2.txt");
	//boolean compare1and2=FileUtils.contentEquals(file1, file2);
	//System.out.println("Are test1.txt and test2.txt the same? "+ compare1and2);
	//}
