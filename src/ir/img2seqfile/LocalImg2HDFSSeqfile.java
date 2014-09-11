package ir.img2seqfile;


import ir.util.HadoopUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

//input: local absolute path of the image folder
//output: path on hdfs where the seqfile should be created
public class LocalImg2HDFSSeqfile {
	private static BytesWritable value=new BytesWritable();
	private static Text key = new Text();
	private static int max_length=1024*1024*20;
	private static byte[] images_raw_bytes=new byte[max_length];

	public static void main(String args[]){
		long startTime = new Date().getTime();
		String img_folder=args[0];
		String seqfile=args[1];
		if(img_folder.startsWith("file://")==false){
			img_folder="file://"+img_folder;
		}
		String[] allfiles=getListOfFiles(img_folder);
		
		
		Path seq_path = null;
		SequenceFile.Writer writer = null;
		long files_processed=0;
        try {
			FileSystem hdfs = FileSystem.get(new Configuration());
			Configuration conf=new Configuration();
			FileSystem localfs=new Path(img_folder).getFileSystem(conf);
			seq_path =new Path(hdfs.getHomeDirectory(),seqfile);
			writer = SequenceFile.createWriter(hdfs,conf,seq_path,key.getClass(),value.getClass());
			for(String file:allfiles){
				if(file.endsWith("~")==false){
					FSDataInputStream fileIn = localfs.open(new Path(file));
	    			int bytes_read=fileIn.read(images_raw_bytes);
	    			if(bytes_read<=0){//skip
	    				fileIn.close();
	    				continue;
	    			}
	    			key.set(file);
	    		//	System.out.println("file:"+file+"\nbytes_read"+bytes_read);
	    			value.set(images_raw_bytes, 0, bytes_read);
	    			writer.append(key, value);
	    			fileIn.close();
	    			files_processed++;
	    			
	    			//output progresss
	    			if(files_processed%5000==0){
	    				System.out.println(files_processed+"/"+allfiles.length+" have been processed");
	    			}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        finally 
        { IOUtils.closeStream( writer); 
        }
        
        System.out.println("all files processed.");
    	long EndTime = new Date().getTime();
		double time_secs=(double)(startTime-EndTime)/1000;
		System.out.println("time spent: "+time_secs+" seconds");
		//writing time results to a file
		FileWriter fw;
		try {
			fw = new FileWriter("LocalImg2HDFSSeqfile_runningtime.txt",true);
			fw.write("input:" + args[0]+"  time spent : "+time_secs+" seconds\n\n");//appends the string to the file
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} //the true will append the new data
		
		
	}

	private static String[] getListOfFiles(String img_folder) {
		// TODO Auto-generated method stub
		ArrayList<String> ListOfFolders=new ArrayList<String>();
		try {
			Path folder_path=new Path(img_folder);
			
			FileSystem fs = folder_path.getFileSystem(new Configuration());
			FileStatus[] status = fs.listStatus(folder_path);
			for (FileStatus filestatus : status){
				if(filestatus.isDir() == false && filestatus.getPath().getName().startsWith("_") == false)
					ListOfFolders.add(img_folder+"/"+filestatus.getPath().getName());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] res = new String[ListOfFolders.size()];
		res = ListOfFolders.toArray(res);
		return res;
	}

}
