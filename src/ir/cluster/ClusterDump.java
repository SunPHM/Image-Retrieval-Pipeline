package ir.cluster;

import ir.util.HadoopUtil;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mahout.clustering.iterator.ClusterWritable;


public class ClusterDump {
	public static void main(String args[]) throws IOException, InterruptedException{
		//run_clusterpp("data/cluster/top/clusteredPoints", "data/cluster/tmpmid/");
		//HadoopUtil.delete("temptemp");
		//TopDownClustering.merge("data/cluster/level/res", "temptemp");
		HadoopUtil.copyMerge("test/cluster/bot/temp/", "test/cluster/clusters.txt");
	}
	

//	public static void run
	//read in the classified points in multiple locations  and output them to a single file
	public static void run_clusterdump(String[] inputs, String output){
		HadoopUtil.delete(output);
		System.out.println("output folder: " + output);
		JobConf conf = new JobConf(ClusterDump.class);
		conf.setJobName("clusterdump");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(ClusterDump.ClusterDumpMap.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);

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
	
	public static class ClusterDumpMap extends MapReduceBase implements Mapper<IntWritable, ClusterWritable, Text,  Text> {
		@Override
		public void map(IntWritable key, ClusterWritable value, OutputCollector<Text, Text> output, Reporter reporter) 
				throws IOException {
			System.out.println(value.getValue().toString());
			output.collect(new Text(value.getValue().toString()), new Text(""));
		}
	}
}
