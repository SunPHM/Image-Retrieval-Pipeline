package ir.cluster;

import ir.feature.FeatureExtraction;
import ir.feature.SIFTExtraction;
import ir.feature.FeatureExtraction.FEMap;
import ir.util.HadoopUtil;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.VectorWritable;


public class clusterpp {
	public static void main(String args[]) throws IOException, InterruptedException{
		//run_clusterpp("data/cluster/top/clusteredPoints", "data/cluster/tmpmid/");
		//run_clusterdump("data/cluster/level/res","data/level/clusterdumptmp/");
		TopDownClustering.merge("data/cluster/level/res", "temptemp");
	}
	
	static class MultiFileOutput extends MultipleSequenceFileOutputFormat<LongWritable, VectorWritable> {
        protected String generateFileNameForKeyValue(LongWritable key, VectorWritable value,String name) {
                return key.toString()+"/"+"part-m-0";
        }
	}
	//mapreduce job to read in the classified points and output them to different sequencefiles depending on their keys 
	public static void run_clusterpp(String input, String output){
		HadoopUtil.delete(output);

		JobConf conf = new JobConf(clusterpp.class);
		conf.set("outputDir", output);
		conf.setJobName("clusterpp");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(VectorWritable.class);

		conf.setMapperClass(clusterppMap.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(MultiFileOutput.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("clusterpp is done");
		
	}

	public static class clusterppMap extends MapReduceBase implements Mapper<IntWritable, WeightedVectorWritable, LongWritable, VectorWritable> {
		LongWritable out_key=new LongWritable();
		VectorWritable vw=new VectorWritable();
		@Override
		public void map(IntWritable key, WeightedVectorWritable value, OutputCollector<LongWritable, VectorWritable> output, Reporter reporter) throws IOException {
			out_key.set(key.get());
			vw.set(value.getVector());
			output.collect(out_key, vw);
		}

	}
	//foreach key output to a seperate file
	public static class clusterppReduce extends MapReduceBase implements Reducer<LongWritable, VectorWritable, LongWritable, VectorWritable> {
		private static String outputDir;
		public void configure(JobConf job) {
		    outputDir=job.get("outputDir");
		    
		}
		@Override
		public void reduce(LongWritable key, Iterator<VectorWritable> values,
				OutputCollector<LongWritable, VectorWritable> output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
				while (values.hasNext()) {
					output.collect(key, values.next());
				}
			   
		}	
	}
	
	//read in the classified points in multiple locations  and output them to a single file
	public static void run_clusterdump(String[] inputs, String output){
		HadoopUtil.delete(output);

		JobConf conf = new JobConf(clusterpp.class);
		//conf.set("outputDir", output);
		conf.setJobName("clusterdump");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(WeightedVectorWritable.class);

		conf.setMapperClass(clusterdumpMap.class);
		conf.setReducerClass(clusterpp.clusterdumpReduce.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    conf.setNumReduceTasks(1);

		//FileInputFormat.setInputPaths(conf, new Path(input));
		for(String input:inputs){
			FileInputFormat.addInputPath(conf, new Path(input));
		}
		
		FileOutputFormat.setOutputPath(conf, new Path(output));

		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("clusterdump is done");
		
	}
	public static class clusterdumpMap extends MapReduceBase implements Mapper<IntWritable, WeightedVectorWritable, LongWritable,  WeightedVectorWritable> {
		@Override
		public void map(IntWritable key, WeightedVectorWritable value, OutputCollector<LongWritable,  WeightedVectorWritable> output, Reporter reporter) 
				throws IOException {
			output.collect(new LongWritable(key.get()),value );
		}

	}
	//foreach key output to a seperate file
	public static class clusterdumpReduce extends MapReduceBase implements Reducer<LongWritable,  WeightedVectorWritable, LongWritable,  WeightedVectorWritable> {
		@Override
		public void reduce(LongWritable key, Iterator< WeightedVectorWritable> values,
				OutputCollector<LongWritable,  WeightedVectorWritable> output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
				while (values.hasNext()) {
					output.collect(key, values.next());
				}
			   
		}	
	}
}
