package org.yyw.HadoopEDF.ParallelProcessing;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class WritetoHDFS {

	  public void WriteToHDFS(String file1, String contents) throws IOException{
		  Configuration conf = new Configuration();
		  FileSystem fs = FileSystem.get(URI.create(file1), conf);
		  Path path = new Path(file1);
		  FSDataOutputStream out = fs.create(path);
		  out.writeBytes(contents);
		  out.writeBytes("\n");
		  out.flush();
		  out.close();
	  }
}
