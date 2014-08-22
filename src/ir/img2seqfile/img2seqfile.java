package ir.img2seqfile;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



/*convert images to a sequence file*/
// input: folder where images are put
//output: seqfile 
import ir.feature.*;
import ir.util.HadoopUtil;

//args[0]: input folder
//args[1]:  outputfolder
public class img2seqfile {
	static Integer splitsize=1024*1024*64;
	static int max_length=1024*1024*20;//max size of image buffer
	public static void main(String args[]) throws IOException{
		long startTime = new Date().getTime();
		runImag2Seqfile(args[0],args[1]+"/temptemp",args[1]);
		//runImag2Seqfile("data/images","output/temp_MR_folder","output/seqfile");
		
		long EndTime = new Date().getTime();
		double time_secs=(double)(startTime-EndTime)/1000;
		System.out.println("time spent: "+time_secs+" seconds");
		FileWriter fw = new FileWriter("Image2Seqfile_runningtime.txt",true); //the true will append the new data
		fw.write("input:" + args[0]+"  time spent : "+time_secs+" seconds\n\n");//appends the string to the file
		fw.close();
		
	}
	public static void runImag2Seqfile(String inFolder,String temp_MR_folder, String outFile){
		
		
		//String MRout="";
		runImg2Seqfile_MRJob(inFolder,outFile);
	//	HadoopUtil.copyMerge(temp_MR_folder, outFile);
	}
	public static void runImg2Seqfile_MRJob(String inFolder, String outFolder){
		
		Configuration conf=new Configuration();		
		//pass the parameters
		conf.set("inFolder", inFolder);
		
		conf.set("mapred.max.split.size", splitsize.toString());
		Job job=null;
		try {
			job = new Job(conf);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
			return;
		}
		
		job.setJobName("runImg2Seqfile");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setJarByClass(img2seqfile.class);
		job.setMapperClass(img2seqfile.readIn_Map.class);
		job.setInputFormatClass(ImageInputFormat.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		try {
			FileInputFormat.setInputPaths(job, new Path(inFolder));
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		FileOutputFormat.setOutputPath(job, new Path(outFolder));
		try {
			try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static class readIn_Map extends  Mapper<Text,LongWritable,  Text, BytesWritable> {
		public static String img_folder = null;
		public static String fn = null;
		public static String feature_folder =null;
		private static BytesWritable outValue=new BytesWritable();
		private static Text outKey = new Text();
		private static Configuration conf=null;
		private static byte[] images_raw_bytes=new byte[max_length];
		
		@Override
		public void setup( Context context) {
			conf=context.getConfiguration();
		   img_folder=conf.get("inFolder");
		   conf=context.getConfiguration();
		}

		@Override
		public void map( Text key,LongWritable value, Context context) throws IOException {
				
			// get the image path
			String filePath = img_folder + "/" + key.toString();
			// extract the SIFT features
			FileSystem fs = FileSystem.get(conf);
					
			outKey.set(filePath);
			FSDataInputStream fileIn = fs.open(new Path(filePath));
			int bytes_read=fileIn.read(images_raw_bytes);
			if(bytes_read!=value.get()){
				outKey.set(filePath+"not_equal_check_please");
			}
			outValue.set(images_raw_bytes, 0, bytes_read);
			try {
				context.write(outKey, outValue);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
						
			
		}
	}
}
