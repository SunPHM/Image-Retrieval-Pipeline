package ir.cluster;

import java.io.IOException;

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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

import ir.util.HadoopUtil;

public class Transform_seq {
	/**run a map-reduce job to transfer a folder of features into a single Sequence File 
	 * @throws IOException */
	
	public static void main(String[] args) {
		run("test/data/features.txt", "test/data/fs.seq", "test/temp/seq");
	}
	
	public static void run(String features, String fs, String temp) {
		HadoopUtil.delete(fs);
		HadoopUtil.delete(temp);
		try {
			runCleanMR(features, temp);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HadoopUtil.copyMerge(temp, fs);
		
	}
	
	public static void runCleanMR(String infolder, String outfile) throws IOException{
		

		JobConf conf = new JobConf(Transform_seq.class);
		conf.setJobName("Transform_seq");
		
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(VectorWritable.class);
		conf.setMapperClass(PPMap.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(SequenceFileOutputFormat.class);
	    
		FileInputFormat.setInputPaths(conf, new Path(infolder));
		FileOutputFormat.setOutputPath(conf, new Path(outfile));
		
		JobClient.runJob(conf);
		
		System.out.println("transformation from features to a sequence file is done");
	}
	
	public static class PPMap extends MapReduceBase implements Mapper<Text, VectorWritable, LongWritable, VectorWritable> {
		
		public static int feature_length = 128;
		public static long recNum = 0;
		
		@Override
		public void map(Text key, VectorWritable value, OutputCollector<LongWritable, VectorWritable> output, Reporter reporter) throws IOException {
			output.collect(new LongWritable(recNum++), value);
		}
		
	}
}