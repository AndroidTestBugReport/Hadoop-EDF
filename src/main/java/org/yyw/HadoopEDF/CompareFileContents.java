package org.yyw.HadoopEDF;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

public class CompareFileContents {

	public static void main(String[] args) throws IOException{
		
		String parallel="parallel/";
		String sequential="sequential/";
		File dir1 = new File(parallel);
		File dir2 = new File(sequential);
		
		getDiff(dir1,dir2);
	}

	private static void getDiff(File dir1, File dir2) throws IOException{
		// TODO Auto-generated method stub
		// scan the first level of folder
		File[] fileList1 = dir1.listFiles();
		File[] fileList2 = dir2.listFiles();
		for(int i=0;i<fileList1.length;i++) {
			//scan the subfolder of that level
			File[] subfileList1 = null;
			File[] subfileList2 = null;
			
			if(fileList1[i].isDirectory() && fileList2[i].isDirectory() ) {
				subfileList1 = fileList1[i].listFiles();
				subfileList2 = fileList2[i].listFiles();
			}
			for(int j=0;j<subfileList1.length;j++) {
				//a and b are the path as well
				String a =subfileList1[i].toString();
				String b =subfileList2[i].toString();
				//pay attention the difference of name between parallel and sequential
				String[] testparallel=a.split("/");
				String[] testsequential=b.split("/");
				String edf_name1=testparallel[2];
				String edf_name2=testsequential[2];
				
				String[] test1=edf_name1.split("-");
                //name1 and name2 are the name
				String name1=test1[0]+".json";
				String name2 =edf_name2;
				System.out.println(name1);
				System.out.println(name2);
                if(name1.equals(name2)) {
                    System.out.println("subfile's name is same");
                    File file1 = new File(a);
                    File file2 = new File(b);
                    boolean compare1and2=FileUtils.contentEquals(file1, file2);
                    System.out.println("Are "+a+" and "+b+" the same? "+ compare1and2);
			    }else {
			    	System.out.println("subfile's name is not same: "+"false");
			
		        }
                
		
	}
	}
	}
	}

//if(args.length !=2) throw (new RuntimeException("Usage: java FileCompare parallel sequential"));{
//    String parallel=args[0];
//    String sequential=args[1];
    
//File file1 = new File("file1.txt");
//File file2 = new File("file2.txt");
//boolean compare1and2=FileUtils.contentEquals(file1, file2);
//System.out.println("Are test1.txt and test2.txt the same? "+ compare1and2);
//}
