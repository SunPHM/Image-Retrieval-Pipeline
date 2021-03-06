package ir.feature;
//input and output are seqfiles
import ir.akm.kmeans_init;
import ir.util.HadoopUtil;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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
	public static String seqfile = "data/images";
	public static String feature_folder = "test/data/features.txt";
	public static final Integer split_size = (int) (1024*1024*14);//30MB
	
	public static void main(String[] args) throws IOException {
		String input = args[0]; // a folder of .seq files
		String output = args[1]; // a path to out put features
		ArrayList<Long> features_nums = new ArrayList<Long>();
		
		String input_seqfiles[] = HadoopUtil.getListOfFiles(input);
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("FE.log")));
		for(String input_seqfile : input_seqfiles){
		
			long startTime = new Date().getTime();
			
			String output_path = output + "/" + new Path(input_seqfile).getName();
			HadoopUtil.mkdir(output_path);
			extractFeatures(input_seqfile, output_path + "/features" );
			
			long endTime = new Date().getTime();
			
			long feature_num = kmeans_init.getFeatureCount( output_path + "/features", new Configuration());
			System.out.println(feature_num);
			
			features_nums.add(feature_num);
			bw.write("For seqfile : " + input_seqfile + ", the output features folder: " + output_path + ", "
					+ "number of features: " + feature_num + ", Time Spent : " + (double)(endTime - startTime) / (1000*60) );
			bw.newLine();
			bw.flush();
		}
		bw.flush();
		bw.close();
		
		Collections.sort(features_nums);
		for(Long num : features_nums)
			System.out.println(num);
	}
	
	public static void extractFeatures(String in_seqfile,  String features){ // the main entry point for Feature Extraction to be called
		//no need for temp folders to put feautres
		seqfile=in_seqfile;
		feature_folder = features;
		HadoopUtil.delete(features);
		extractMR(seqfile, features);
		System.out.println("feature extraction is done, featres output to " + features);
		
	}
	
	// extract features using Map-Reduce
	public static void extractMR(String infile, String outfile){
		HadoopUtil.delete(outfile);
		Configuration conf=new Configuration();
		
		//pass the parameters
		conf.set("seqfile", seqfile);
		//conf.set("fn", fn);
		conf.set("feature_folder", feature_folder);
		conf.set("outfile","test");
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
		job.setJarByClass(FeatureExtraction_seq.class);
		job.setMapperClass(FeatureExtraction_seq.FEMap.class);
		
//		job.setReducerClass(FeatureExtraction_seq.FEReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		
		///debug
//		job.setNumReduceTasks(0);
	
		System.out.println("\nInfo Kmeans: !!!!Setting number of reducer adaptively!!!");
	    int default_num_reducer = 100;
	    try {
				FileSystem fs = FileSystem.get(conf);
				ContentSummary cs =fs.getContentSummary(new Path(infile));
				long input_size=cs.getLength();//cs.getSpaceConsumed();
				default_num_reducer=(int)(Math.ceil( ((double)input_size)/(1024*1024*64) ));//50MB PER REducer
				System.out.println("Path: "+infile+" size "+input_size+", will use "+default_num_reducer+" reducer(s)\n\n");
		} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
		}
		//job.setNumReduceTasks(default_num_reducer);
	    job.setNumReduceTasks(0);
		

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
//		job.setNumReduceTasks(0);
		
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
		private static final int feature_length=128;
		public static String outfile=null;
		//used for feature count
		static Path feature_count_path = null;
		static long feature_count = 0;
		static String featurecount_filename = null;
		
		static VectorWritable vw = new VectorWritable();
		static Vector vec = new DenseVector(feature_length);
		
		//debug test
//		static float[] testarray = null;

		
//		private MultipleOutputs<Text, VectorWritable> mos;
		@Override
		public void setup( Context context) {
			Configuration conf=context.getConfiguration();
		   img_folder=conf.get("seqfile");
		//   fn=job.get("fn");
		   feature_folder=conf.get("feature_folder");
		   //outfile=conf.get("outfile");
		   
		   //used for feature count
		   Path p = new Path(feature_folder);
		   Path parent = p.getParent();
		   feature_count_path = new Path(parent, "feature_count");	
		   feature_count = 0;
		   
//		   testarray = new float[1000*1000*128];
		}

		@Override
		public void map( Text key,BytesWritable value, Context context) throws IOException {
				
			// get the image path
			String file = img_folder + "/" + key.toString();
			// extract the SIFT features
			//FileSystem fs = FileSystem.get(new Configuration());
			
			//assuming each pic has a different name
			if(featurecount_filename == null){
				featurecount_filename = new Path( key.toString()).getName();
			}
			try{
				byte[] image_bytes=value.getBytes();
				if(image_bytes.length>0){
					BufferedImage img = ImageIO.read(new ByteArrayInputStream(value.getBytes()));
					String[] features = SIFTExtraction.getFeatures(img);
					for(int i = 0; i < features.length; i++){
						///TODO change here to choose if use byte representation
						double[]  feature = getPoints(features[i].split(" "), feature_length);
						
				//		System.out.println(features[i]);
//						normalize(feature);


						vec.assign(feature);
						vw.set(vec);
						context.write(new Text(file), vw);
						feature_count ++;
						//mos.write("seq",new Text(file), vw, outfile);
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
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch(java.lang.ArrayIndexOutOfBoundsException e){
				//what can we do about the corrupted images?????
			}
			
		}
		/*
		// normalize the feature
		private void normalize(double[] feature) {
			// TODO Auto-generated method stub
			double sum = 0;
			for(int i = 0; i < feature.length; i ++){
				sum = sum + feature[i] * feature[i];
			}
			sum = Math.sqrt(sum);
			for(int i = 0; i < feature.length; i ++){
				feature[i] = feature[i] / sum;
			}
		}
		*/
		//write the feature_count to the feature_count_path/file_name
		protected void cleanup(Context context) throws IOException {
			if(feature_count == 0){
				return;
			}
			Configuration conf = context.getConfiguration();
			FileSystem fs =FileSystem.get(conf);
			FSDataOutputStream writer = fs.create(new Path(feature_count_path, featurecount_filename));
			StringBuilder sb=new StringBuilder();
			sb.append("" + feature_count);
			byte[] byt=sb.toString().getBytes();
			writer.write(byt);
			writer.close();
		}

		public static double[] getPoints(String[] args, int size){// get the feature vector from the 
			//System.out.println(args.length);
			double[] points = new double[size];
			for (int i = 0; i < size; i++)
				points[i] = Double.parseDouble(args[i+4]);
			return points;
		}
		public static double[] getPoints_bytes(String[] args, int size){// get the feature vector from the 
			//System.out.println(args.length);
			double[] points = new double[size];
			
			//get the abs largest value of the features
			double max = 0;
			for (int i = 0; i < size; i++){
				points[i] = Double.parseDouble(args[i+4]);
				if(max < Math.abs(points[i])){
					max = Math.abs(points[i]);
				}
			}
			
			//convert them to bytes
			for(int i = 0; i < size; i ++){
				points[i] = (int) (127 * points[i] / max);
			}
			return points;
		}
	}
}


class testCopyMerge {
	public static void test(String seqfile){
		Configuration config = new Configuration();
		Path path = new Path(seqfile);
		WritableComparable key=null;
		Writable value =null;
		try{
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), path, config);
			 key = (WritableComparable) reader.getKeyClass().newInstance();
			 value = (Writable) reader.getValueClass().newInstance();
			while (reader.next(key, value)){
				//do nothing
				System.out.print("\t"+key.toString());
			}
			reader.close();
		}catch(IOException e){
			System.out.println(key.toString()+"\t"+value.toString());
			e.printStackTrace();
		}
		catch(InstantiationException e){
			System.out.println(key.toString()+"\t"+value.toString());
			e.printStackTrace();
		}
		catch(IllegalAccessException e){
			System.out.println(key.toString()+"\t"+value.toString());
			e.printStackTrace();
		}
		
	}
}