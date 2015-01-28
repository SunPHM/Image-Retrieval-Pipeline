package ir.img2seqfile;

import ir.util.HadoopUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/*
 * convert a folder of tar files (which has images in it) to a output folder of seq files
 * for each tar file, create a seqfile 
 * */
public class Tars2Seq {
	private static BytesWritable value=new BytesWritable();
	private static Text key = new Text();
	private static int max_length=1024*1024*20;
	private static byte[] images_raw_bytes=new byte[max_length];
	
	public static void testoutputseqfile() throws IOException, InstantiationException, IllegalAccessException{
		String input_seqfile = "/home/xiaofeng/Downloads/test/test_out/n01440764.tar.seq";
		Configuration config = new Configuration();
		Path path = new Path(input_seqfile);
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), path, config);
		key = (Text) reader.getKeyClass().newInstance();
		 value = (BytesWritable) reader.getValueClass().newInstance();
		while (reader.next(key, value)){
		  // perform some operating
			FileOutputStream fos = new FileOutputStream(new File("/home/xiaofeng/Downloads/test/" + key.toString()));
					fos.write(value.get(), 0, value.get().length);
					fos.flush();
					fos.close();
		}
		reader.close();
	}
	
	//input : folder of tar files
	//output: folder of seqfiles accordingly
	public static void main( String[] args) throws IOException, InstantiationException, IllegalAccessException  { 
//		testoutputseqfile();
//		if(true)
//			return;
		
		String inputfolder = args[0];//"/home/xiaofeng/Downloads/test/test";//
		String outputfolder = args[1]; // "/home/xiaofeng/Downloads/test/test_out";
		
		long startTime = new Date().getTime();
		
		File output = new File(outputfolder);
		if(!output.mkdir()){
			System.out.println("failed to create dir: " + outputfolder + ", exit");
			return;
		}
		String[] tarfiles = HadoopUtil.getListOfFiles(inputfolder);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//process the tar files one by one
		TarArchiveInputStream myTarFile = null;
		int offset;
		TarArchiveEntry entry = null;
		String filename = null;
		
		long num_images = 0;
		int tar_files_processed = 0;
		for(String tarfile : tarfiles){
			if(tar_files_processed % 50 == 0){
				try {
					System.out.println("Sleepting for 3 secs");
					Thread.sleep(1000*3);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			//skip non-tar files
			if(tarfile.endsWith(".tar") == false){
				continue;
			}
			//skip corrupted tar files as possible
			//for each tar file write out to a seqfile
            try {
            	Path outfile = new Path(outputfolder + "/" + new Path(tarfile).getName() + ".seq");
            	SequenceFile.Writer writer = SequenceFile.createWriter( fs, conf, outfile, key.getClass(), value.getClass());;
            	
            	myTarFile=new TarArchiveInputStream(new FileInputStream(new File(tarfile)));
            	while ((entry = myTarFile.getNextTarEntry()) != null) {
                    /* Get the name of the file */
                    filename = entry.getName();
                    byte[] content = new byte[(int) entry.getSize()];
                   // int num_bytes = (int) entry.getSize();
                    offset=0;
                    myTarFile.read(content, offset, content.length - offset);
              
                    /* Define OutputStream for writing the file */
                   // FileOutputStream outputFile = new FileOutputStream(new File(outputfolder + "/" + filename));
                    /* Use IOUtiles to write content of byte array to physical file */
                    //IOUtils.write(content,outputFile);              
                    /* Close Output Stream */
                   // outputFile.close();
                    
                    key.set(filename);
                    value.set(content, 0, content.length);
                    writer.append(key, value);
                    num_images ++;
                    if(num_images % 500000 == 0){
                    	System.out.println("num of images processed : " + num_images);
                    	
                    }
                    
                    }
            	writer.close();
            	
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("Open tar file error with file : " + tarfile);
				e.printStackTrace();
				continue;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
		}
		long EndTime = new Date().getTime();
		double time_secs=(double)(0 - startTime + EndTime)/1000;
		System.out.println("time spent: "+time_secs+" seconds\n Total images: " + num_images);
		

	}

}
