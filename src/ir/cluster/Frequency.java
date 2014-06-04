package ir.cluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
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
	
	public static void main(String[] args) throws IOException{
		//getNames("/home/hadoop/bovw/code/resources/features_new", "data/fnames.txt");
		//clusters = args[0];
		String data = "data/tf.txt";
		HadoopUtil.delete("bw");
		run(Search.fnames, "bw");
		HadoopUtil.copyMerge("bw", data);
		//readClusters("/home/hadoop/Desktop/clusters.txt");
	}
	
	public static void getNames(String features, String file) throws FileNotFoundException{
		File folder = new File(features);
		String[] list = folder.list();
		PrintWriter pw = new PrintWriter(file);
		for(int i = 0; i < list.length; i++){
			pw.println(list[i]);
		}
		pw.close();
	}
	
	public static void run(String infile, String outfile) throws IOException{

		JobConf conf = new JobConf(FrequencyExtractor.class);
		conf.setJobName("FrequencyExtractor");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(FEMap.class);
		conf.setReducerClass(FEReduce.class);
		conf.setNumReduceTasks(1);
		
		conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    
		FileInputFormat.setInputPaths(conf, new Path(infile));
		FileOutputFormat.setOutputPath(conf, new Path(outfile));
		
		JobClient.runJob(conf);
	}
	
	public static class FEMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			double[][] cs = readClusters(Search.clusterFile);//read clusters
			int[] marks = new int[Search.clusterNum];
			// read the features and 
			String file = value.toString();
			
			String data = Search.featureFolder + file;
			//read the file line by line
			Path dp = new Path(data);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream input = fs.open(dp);
			String line;
			while((line = input.readLine()) != null){
				double[] feature = new double[Search.featureSize];
				String[] args = line.split(" ");
				for (int i = 0; i < Search.featureSize; i++)
					feature[i] = Double.parseDouble(args[i + 10]);
				int index = findBestCluster(feature, cs);
				marks[index]++;
			}
			
			String result = "";
			int num = 0;
			for(int i = 0; i < Search.clusterNum; i++){
				for(int j = 0; j < marks[i]; j++){
					if(result.length() == 0) result += i;
					else result += " " + i;
					num++;
				}	
			}
			
			output.collect(new Text(file), new Text(num + "\t" + result));
			
			System.out.println(file + " processed");
		}
		
		public static double[][] readClusters(String clusters) throws IOException{
			//TODO: in current stage, I only concern about the cluster centers not the radiuses
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(clusters);
			FSDataInputStream input = fs.open(path);
			double[][] cs = new double[Search.clusterNum][Search.featureSize];
			String line;
			for(int i = 0; i < Search.clusterNum; i ++){
				line = input.readLine();
				//System.out.println(line);
				String center = line.split("\\]")[0].split("c=\\[")[1];
				String[] array = center.split(", ");
				for(int j = 0; j < Search.featureSize; j++)
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

	public static class FEReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			String sum = "";
			while (values.hasNext()) {
				String s = values.next().toString();
				if(!s.equals(""))
					if (sum.length() == 0 ) sum = s;
					else sum = sum + "\t" + s;
			}
			//System.out.println(sum);
			output.collect(key, new Text(sum));
			//yc++;
		}
		
	}
	
}