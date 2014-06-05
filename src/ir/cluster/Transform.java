package ir.cluster;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import ir.util.HadoopUtil;

public class Transform {
	/**run a map-reduce job to transfer a folder of features into a single Sequence File 
	 * @throws IOException */
	
	public static void main(String[] args) throws IOException{
		run();
	}
	
	public static void run() throws IOException{
		runCleanMR("data/features", "data/temp/seq");
		HadoopUtil.copyMerge("data/temp/seq", "data/cluster/fs.seq");
		HadoopUtil.delete("data/temp/seq");
	}
	
	public static void runCleanMR(String infolder, String outfile) throws IOException{
		

		JobConf conf = new JobConf(Transform.class);
		conf.setJobName("Transform");
		
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(VectorWritable.class);
		
		conf.setMapperClass(PPMap.class);
		conf.setReducerClass(PPReduce.class);
		conf.setNumReduceTasks(1);
		
		conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(SequenceFileOutputFormat.class);
	    
		FileInputFormat.setInputPaths(conf, new Path(infolder));
		FileOutputFormat.setOutputPath(conf, new Path(outfile));
		
		JobClient.runJob(conf);
		
		System.out.println("transformation from a folder of features to a sequence file is done");
	}
	
	public static class PPMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, VectorWritable> {
		
		public static int feature_length = 128;
		public static long recNum = 0;
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, VectorWritable> output, Reporter reporter) throws IOException {
			
			double[] feature = getPoints(value.toString().split(" "), feature_length);
			VectorWritable vw = new VectorWritable();
			Vector vec = new DenseVector(feature.length);
			vec.assign(feature);
			vw.set(vec);
			output.collect(new LongWritable(recNum++), vw);
		}
		
		public static double[] getPoints(String[] args, int size){// get the feature vector from the 
			System.out.println(args.length);
			double[] points = new double[size];
			for (int i = 0; i < size; i++)
				points[i] = Double.parseDouble(args[i + 4]);
			return points;
		}
	}
	
	public static class PPReduce extends MapReduceBase implements Reducer<LongWritable, VectorWritable, LongWritable, VectorWritable> {

		@Override
		public void reduce(LongWritable key, Iterator<VectorWritable> values, OutputCollector<LongWritable, VectorWritable> output, 
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			while(values.hasNext()){
				output.collect(key, values.next());
			}
		}
		
		
	}
}