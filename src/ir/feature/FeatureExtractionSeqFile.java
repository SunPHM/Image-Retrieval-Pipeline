package ir.feature;

import ir.util.HadoopUtil;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FeatureExtractionSeqFile {
	public static String seqfile= "data/images";
	//public static String fn = "test/data/fn.txt";
	public static String feature_folder ="test/data/features.txt";
	public static final Integer split_size=1024*1024*10;//10MB
	
	public static void main(String[] args) {
	//	SIFTExtraction.getNames(img_folder, fn);
		extractFeatures(args[0], args[1], args[2]);
	}
	
	public static void extractFeatures(String in_seqfile,  String features, String temp){ // the main entry point for Feature Extraction to be called
		seqfile=in_seqfile;
		feature_folder = features;
		HadoopUtil.delete(temp);
		extractMR(seqfile, temp);
		HadoopUtil.copyMerge(temp, feature_folder);
		System.out.println("feature extraction is done");
	}
	
	// extract features using Map-Reduce
	public static void extractMR(String infile, String outfile){
		HadoopUtil.delete(outfile);
		Configuration conf=new Configuration();		
		//pass the parameters
		conf.set("seqfile", seqfile);
		//conf.set("fn", fn);
		conf.set("feature_folder", feature_folder);
//		conf.set("mapred.max.split.size", split_size.toString());
		Job job=null;
		try {
			job = new Job(conf);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
			return;
		}
		
		job.setJobName("FeatureExtractionSeqFile");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(FeatureExtractionSeqFile.class);
		job.setMapperClass(FeatureExtractionSeqFile.FEMap.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
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
	}
	
	public static class FEMap extends  Mapper<Text,BytesWritable,  Text, Text> {
		public static String img_folder = null;
		public static String fn = null;
		public static String feature_folder =null;
		
		@Override
		public void setup( Context context) {
			Configuration conf=context.getConfiguration();
		   img_folder=conf.get("seqfile");
		//   fn=job.get("fn");
		   feature_folder=conf.get("feature_folder");
		}

		@Override
		public void map( Text key,BytesWritable value, Context context) throws IOException {
				
			// get the image path
			//String file = img_folder + "/" + key.toString();
			// extract the SIFT features
			FileSystem fs = FileSystem.get(new Configuration());
				BufferedImage img = ImageIO.read(new ByteArrayInputStream(value.get()));
				String[] features = SIFTExtraction.getFeatures(img);
				// store them into a file
				for(int i = 0; i < features.length; i++){
					try {
						context.write(key, new Text(features[i]));
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

		}
	}

}
