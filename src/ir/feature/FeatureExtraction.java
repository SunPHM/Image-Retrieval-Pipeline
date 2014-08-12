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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import ir.util.HadoopUtil;

public class FeatureExtraction {
	
	public static String img_folder = "data/images";
	public static String fn = "test/data/fn.txt";
	public static String feature_folder ="test/data/features";
	
	public static void main(String[] args) {
		SIFTExtraction.getNames(img_folder, fn);
		extractMR(fn, "data/temp/temp");
	}
	
	public static void extractFeatures(String images, String fn0, String features, String temp){ // the main entry point for Feature Extraction to be called
		img_folder = images;
		feature_folder = features;
		fn = fn0;
		HadoopUtil.delete(feature_folder);
		SIFTExtraction.getNames(images, fn);
		extractMR(fn, temp);
	}
	
	// extract features using Map-Reduce
	public static void extractMR(String infile, String outfile){

		HadoopUtil.delete(outfile);

		JobConf conf = new JobConf(FeatureExtraction.class);
		conf.setJobName("FeatureExtraction");
		
		//pass the parameters
		conf.set("img_folder", img_folder);
		conf.set("fn", fn);
		conf.set("feature_folder", feature_folder);
		//

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(FEMap.class);

		conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(infile));
		FileOutputFormat.setOutputPath(conf, new Path(outfile));

		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("feature extraction is done");
	}
	
	public static class FEMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public static String img_folder = null;
		public static String fn = null;
		public static String feature_folder =null;
		
		@Override
		public void configure(JobConf job) {
		   img_folder=job.get("img_folder");
		   fn=job.get("fn");
		   feature_folder=job.get("feature_folder");
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
				
			// get the image path
			String file = img_folder + "/" + value.toString();
			// extract the SIFT features
			FileSystem fs = FileSystem.get(new Configuration());
			try{
				BufferedImage img = ImageIO.read(fs.open(new Path(file)));
				String[] features = SIFTExtraction.getFeatures(img);
				// store them into a file
				store(features, feature_folder + "/" + value.toString() + ".txt");
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
