package ir.feature;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.PrintWriter;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ir.util.HadoopUtil;

public class FeatureExtractionCombineFileInputFormat {
	
	public static String img_folder = "data/images";
	//public static String fn = "test/data/fn.txt";
	public static String feature_folder ="test/data/features";
	
	public static void main(String[] args) {
	//	SIFTExtraction.getNames(img_folder, fn);
		extractFeatures(args[0], args[1], args[2]);
	}
	
	public static void extractFeatures(String images,  String features, String temp){ // the main entry point for Feature Extraction to be called
		img_folder = images;
		feature_folder = features;
		//fn = fn0;
		HadoopUtil.delete(feature_folder);
		//SIFTExtraction.getNames(images, fn);
		extractMR(images, temp);
	}
	
	// extract features using Map-Reduce
	public static void extractMR(String infile, String outfile){

		HadoopUtil.delete(outfile);

		Configuration conf=new Configuration();
		
		
		//pass the parameters
		conf.set("img_folder", img_folder);
		//conf.set("fn", fn);
		conf.set("feature_folder", feature_folder);
		conf.set("mapred.max.split.size", "13421772");
		Job job=null;
		try {
			job = new Job(conf);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
			return;
		}
		
		job.setJobName("FeatureExtraction");
		//

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(FeatureExtractionCombineFileInputFormat.class);
		job.setMapperClass(FeatureExtractionCombineFileInputFormat.FEMap.class);

		job.setInputFormatClass(ImageInputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		try {
			FileInputFormat.setInputPaths(job, new Path(infile));
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, new Path(outfile));

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

		System.out.println("feature extraction is done");
	}
	
	public static class FEMap extends  Mapper<Text,LongWritable,  Text, Text> {
		public static String img_folder = null;
		public static String fn = null;
		public static String feature_folder =null;
		
		@Override
		public void setup( Context context) {
			Configuration conf=context.getConfiguration();
		   img_folder=conf.get("img_folder");
		//   fn=job.get("fn");
		   feature_folder=conf.get("feature_folder");
		}

		@Override
		public void map( Text key,LongWritable value, Context context) throws IOException {
				
			// get the image path
			String file = img_folder + "/" + key.toString();
			// extract the SIFT features
			FileSystem fs = FileSystem.get(new Configuration());
			try{
				BufferedImage img = ImageIO.read(fs.open(new Path(file)));
				String[] features = SIFTExtraction.getFeatures(img);
				// store them into a file
				store(features, feature_folder + "/" + key.toString() + ".txt");
			} catch (java.lang.IllegalArgumentException e){
				System.out.println("the image causing exception: " + file);
			}
			//System.out.println(file + " processed");
		}

		public void store(String[] features, String filename){
			if(features == null) return;			
			try {
				Configuration conf = new Configuration();
				FileSystem fs;
				fs = FileSystem.get(conf);
				Path outFile = new Path(filename);
				FSDataOutputStream out = fs.create(outFile);
				PrintWriter pw = new PrintWriter(out.getWrappedStream());
				for(String feature : features){
					pw.println(filename + "\t" + feature);
					//pw.flush();
				}
				pw.close();
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}
