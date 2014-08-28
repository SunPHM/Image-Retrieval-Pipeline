package ir.feature;
//input and output are seqfiles
import ir.util.HadoopUtil;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class FeatureExtraction_seq {
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
		conf.set("mapred.max.split.size", split_size.toString());
		Job job=null;
		try {
			job = new Job(conf);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
			return;
		}
		
		job.setJobName("FeatureExtractionSeqFile");
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		
		job.setJarByClass(FeatureExtraction_seq.class);
		job.setMapperClass(FeatureExtraction_seq.FEMap.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);

		
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
		
		
		//deciding the number of reduce tasks to use
		int default_num_reducer = 100;
		try {
			FileSystem fs = FileSystem.get(conf);
			ContentSummary cs =fs.getContentSummary(new Path(infile));
			long input_size=cs.getSpaceConsumed();
			default_num_reducer=(int)(Math.ceil((double)input_size/(1024*1024*50)));//50MB PER REducer
			System.out.println("Path: "+infile+" size "+input_size+", will use "+default_num_reducer+" reducer(s)");
		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		job.setNumReduceTasks(default_num_reducer);
		
		
		
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
	
	public static class FEMap extends  Mapper<Text,BytesWritable, Text, VectorWritable> {
		public static String img_folder = null;
		public static String fn = null;
		public static String feature_folder =null;
		private static VectorWritable vw = new VectorWritable();
		private static final int feature_length=128;
		private static Vector vec = new DenseVector(feature_length);
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
			String file = img_folder + "/" + key.toString();
			// extract the SIFT features
			//FileSystem fs = FileSystem.get(new Configuration());
			try{
				byte[] image_bytes=value.getBytes();
				if(image_bytes.length>0){
					BufferedImage img = ImageIO.read(new ByteArrayInputStream(value.getBytes()));
					String[] features = SIFTExtraction.getFeatures(img);
					// store them into a file
					for(int i = 0; i < features.length; i++){
						double[]  feature=getPoints(features[i].split(" "), feature_length);
						vec.assign(feature);
						vw.set(vec);
						context.write(new Text(file), vw);
					}
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
