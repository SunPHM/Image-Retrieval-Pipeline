package ir.feature;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import ir.util.HadoopUtil;

public class FeatureExtraction {
	
	public static String img_folder = "data/images";
	public static final Integer split_size = 1024*1024*20;//30MB
	
	public static void main(String[] args) {
		extractFeatures(args[0], args[1]);
	}
	
	public static void extractFeatures(String images,  String features){ // the main entry point for Feature Extraction to be called
	System.out.println("debuggg111");
		img_folder = images;
		HadoopUtil.delete(features);
		extractMR(images, features);
		System.out.println("feature extraction is done");
	}
	
	// extract features using Map-Reduce
	public static void extractMR(String infile, String outfile){
		HadoopUtil.delete(outfile);
		Configuration conf=new Configuration();		
		//pass the parameters
		conf.set("img_folder", img_folder);
		conf.set("mapred.max.split.size", split_size.toString());
		conf.set("outfile","test");
		
		Job job=null;
		try {
			job = new Job(conf);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
			return;
		}
		
		job.setJobName("FeatureExtraction");
		job.setJarByClass(FeatureExtraction.class);
		job.setMapperClass(FeatureExtraction.FEMap.class);
		//
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(VectorWritable.class);
		job.setInputFormatClass(ImageInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

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
	
	public static class FEMap extends  Mapper<Text,LongWritable, Text, VectorWritable> {
		public static String img_folder = null;
		public static int feature_num = 128;
		public static String outfile=null;
		
		@Override
		public void setup( Context context) {
			Configuration conf=context.getConfiguration();
		   img_folder=conf.get("img_folder");
		   outfile=conf.get("outfile");
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
				for(int i = 0; i < features.length; i++){
					Vector vec = new DenseVector(feature_num);
					VectorWritable vw = new VectorWritable();
					double[] feature= getPoints(features[i].split(" "), feature_num);
					vec.assign(feature);		
					vw.set(vec);
					context.write(new Text(file), vw);
				}
			} catch (java.lang.IllegalArgumentException e){
				System.out.println("the image causing exception: " + file);
			}
			catch( java.awt.color.CMMException e){
				//
				System.out.println("the image causing exception: " + file);
			}
			catch(javax.imageio.IIOException e){
				System.out.println("the image causing exception: " + file);
			}
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public static double[] getPoints(String[] args, int size){// get the feature vector from the 
			//System.out.println(args.length);
			double[] points = new double[size];
			for (int i = 0; i < size; i++)
				points[i] = Double.parseDouble(args[i+4]);
			return points;
		}
	}
}
