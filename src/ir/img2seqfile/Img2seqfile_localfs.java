package ir.img2seqfile;
import ir.util.HadoopUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;



//args0: input images folder
//args1: output seqfile 
	

public class Img2seqfile_localfs {
	
	private static BytesWritable value=new BytesWritable();
	private static Text key = new Text();
	private static int max_length=1024*1024*20;
	private static byte[] images_raw_bytes=new byte[max_length];

	    public static void main( String[] args)  { 
	    	long startTime = new Date().getTime();
	        String output = args[1];
	        Configuration conf = new Configuration();
	        
	        SequenceFile.Writer writer = null;
	        try {
	        	FileSystem fs = FileSystem.get(URI.create( output), conf);
		        Path outfile=new Path(output);
		        
		        String[] allfiles=HadoopUtil.getListOfFiles(args[0]);
		       
		       
	        	writer = SequenceFile.createWriter( fs, conf, outfile, key.getClass(), value.getClass());

	            int files_processed=0;
	            for(String file:allfiles){
	            	if(file.endsWith("~")==false){
		            	FSDataInputStream fileIn = fs.open(new Path(file));
		    			int bytes_read=fileIn.read(images_raw_bytes);
		    			
		    			key.set(file);
		    		//	System.out.println("file:"+file+"\nbytes_read"+bytes_read);
		    			value.set(images_raw_bytes, 0, bytes_read);
		    			writer.append(key, value);
		    			fileIn.close();
		    			files_processed++;
		    			if(files_processed%1000==0){
		    				System.out.println(files_processed+"/"+allfiles.length+ "  have been processed");
		    			}
	            	}
	            }
	            
	        }
	        catch(IOException e){
	        	e.printStackTrace();
	        }
	        finally 
	        { IOUtils.closeStream( writer); 
	        }
	        System.out.println("all files processed.");
	    	long EndTime = new Date().getTime();
			double time_secs=(double)(startTime-EndTime)/1000;
			System.out.println("time spent: "+time_secs+" seconds");
			FileWriter fw;
			try {
				fw = new FileWriter("Image2Seqfile_runningtime.txt",true);
				fw.write("input:" + args[0]+"  time spent : "+time_secs+" seconds\n\n");//appends the string to the file
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} //the true will append the new data
			
	    } 
}
