package ir.cluster;

import ir.util.HadoopUtil;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mahout.clustering.iterator.ClusterWritable;


public class ClusterDump {
	public static void main(String args[]) throws IOException, InterruptedException{
		//run_clusterpp("data/cluster/top/clusteredPoints", "data/cluster/tmpmid/");
		HadoopUtil.delete("temptemp");
		TopDownClustering.merge("data/cluster/level/res", "temptemp");
	}
	

	//read in the classified points in multiple locations  and output them to a single file
	public static void run_clusterdump(String[] inputs, String output){
		HadoopUtil.delete(output);

		JobConf conf = new JobConf(ClusterDump.class);
		conf.setJobName("clusterdump");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(ClusterWritable.class);

		conf.setMapperClass(ClusterDump.ClusterdumpMap.class);
		conf.setReducerClass(ClusterDump.ClusterdumpReduce.class);

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
	
	public static class ClusterdumpMap extends MapReduceBase implements Mapper<IntWritable, ClusterWritable, IntWritable,  ClusterWritable> {
		@Override
		public void map(IntWritable key, ClusterWritable value, OutputCollector<IntWritable,  ClusterWritable> output, Reporter reporter) 
				throws IOException {
			output.collect(key,value );
		}

	}
	//foreach key output to a seperate file
	public static class ClusterdumpReduce extends MapReduceBase implements Reducer<IntWritable,  ClusterWritable, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterator< ClusterWritable> values,OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
				while (values.hasNext()) {
					output.collect(null, new Text(values.next().getValue().toString()));
				}
			   
		}	
	}
}
