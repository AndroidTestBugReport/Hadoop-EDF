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

public class CompareResultsOfSmallfilesOnAWS {

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
				
				}
			}catch(AmazonServiceException e) {
				e.printStackTrace();
			}catch(SdkClientException e) {
				e.printStackTrace();
			}
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