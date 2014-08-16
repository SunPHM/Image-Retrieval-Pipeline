package ir.cluster;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import ir.util.HadoopUtil;


public class Frequency {
	//input: clusters.txt, feature texts
	//output: a file containing both the name of file and the cluster id
	public static int featureSize = 128;
	public static int clusterNum = 100;
	public static String clusters = "";
	public static String features = "";
	
	public static void main(String[] args) throws IOException{
		runJob("test/data/features/", 100, "test/cluster/clusters.txt", "test/temp/freq", "test/data/frequency.txt");
	}
	
	public static void init(String features, int clusterNum, String clusters){
		Frequency.clusterNum = clusterNum;
		Frequency.clusters = clusters;
		Frequency.features = features;
	}
	
	public static void runJob (String features, int clusterNum, String clusters, String temp, String output){
		Frequency.init(features, clusterNum, clusters);
		HadoopUtil.delete(temp);
		HadoopUtil.delete(output);
		runFreMR(features, temp);
		HadoopUtil.copyMerge(temp, output);
	}
	
	public static void runFreMR(String infile, String outfile){
		
		JobConf conf = new JobConf(Frequency.class);
		conf.setJobName("Frequency");
		
		// configure parameters
		conf.set("features", features);
		conf.set("clusterNum", new Integer(clusterNum).toString());
		conf.set("clusters", clusters);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(FreMap.class);
		conf.setReducerClass(FreReduce.class);
		//conf.setNumReduceTasks(1);
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
	}
	
	public static class FreMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public static int featureSize = 128;
		public static int clusterNum = 100;
		public static String clusters = "";
		public static String features = "";
		public static double[][] cs = null;
		
		@Override
		public void configure(JobConf job) {
		   clusterNum = Integer.parseInt(job.get("clusterNum"));
		   clusters = job.get("clusters");
		   features = job.get("features");
		   try {
			cs = readClusters(clusters, clusterNum);
		   } catch (IOException e) {
			   // TODO Auto-generated catch block
			   e.printStackTrace();
		   }//read clusters
		}
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			String file = value.toString().split("\t")[0];
			String line = value.toString().split("\t")[1];
			double[] feature = new double[featureSize];
			String[] args = line.split(" ");
			for (int i = 0; i < featureSize; i++)
				feature[i] = Double.parseDouble(args[i + 4]);
			int index = findBestCluster(feature, cs);
			output.collect(new Text(file), new Text(1 + "\t" + index));
			//System.out.println(file + " processed");
		}
		
		public static double[][] readClusters(String clusters, int clusterNum) throws IOException{
			//TODO: in current stage, I only concern about the cluster centers not the radiuses
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(clusters);
			FSDataInputStream input = fs.open(path);
			double[][] cs = new double[clusterNum][featureSize];
			String line;
			//System.out.println(clusters);
			for(int i = 0; i < clusterNum; i ++){
				line = input.readLine();
				//System.out.println(line);
				String center = line.split("\\]")[0].split("c=\\[")[1];
				String[] array = center.split(", ");
				for(int j = 0; j < featureSize; j++)
					cs[i][j] = Double.parseDouble(array[j]);
			}
			return cs;
		}
		
		public static int findBestCluster(double[] feature, double[][] clusters){
			int index = -1;
			double distance = -1.1;
			//feature = norm(feature);
			for(int i = 0; i < clusters.length; i++){
				double fl = 0;
				double cl = 0;
				double ds = 0;
				for(int j = 0; j < clusters[i].length; j++){
					ds += feature[j] * clusters[i][j];
					fl += feature[j] * feature[j];
					cl += clusters[i][j] * clusters[i][j];
				}
				ds = ds / (Math.sqrt(fl) * Math.sqrt(cl));
				
				if(ds > distance){
					distance = ds;
					index = i;
				}
			}
			return index;
		}
		
		public static double[] norm(double[] feature){
			double len = 0;
			for(int i = 0; i < feature.length; i++){
				len += feature[i] * feature[i];
			}
			len = Math.sqrt(len);
			for(int i = 0; i < feature.length; i++){
				feature[i] /= len;
			}
			return feature;
		}
	}

	public static class FreReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			String sum = "";
			int total = 0;
			while (values.hasNext()) {
				String s = values.next().toString();
				if(!s.equals(""))
					if (sum.length() == 0) sum = s.split("\t")[1];
					else sum = sum + " " + s.split("\t")[1];
					total += Integer.parseInt(s.split("\t")[0]);
			}
			//System.out.println(sum);
			output.collect(key, new Text(total + "\t" + sum));
		}
	}
	
}