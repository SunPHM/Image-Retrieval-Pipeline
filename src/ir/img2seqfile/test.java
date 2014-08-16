package ir.img2seqfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class test {
  public static void main(String args[]) throws IOException, InstantiationException, IllegalAccessException{
	  Configuration config = new Configuration();
	  Path path = new Path("output/seqfile");
	  
	  SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), path, config);
	  WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
	  Writable value = (Writable) reader.getValueClass().newInstance();
	  while (reader.next(key, value))
		//  FSDataOutputStream fsout=new FSDataOutputStream(new OutputStream()) 
	  reader.close();
  }
}
