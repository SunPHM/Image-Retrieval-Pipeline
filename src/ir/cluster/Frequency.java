package ir.cluster;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
	public static int clusterNum = 0;
	public static String clusters = "";
	public static String features = "";
	
	public static void main(String[] args) throws IOException{
		runJob("data/features/", 100, "data/cluster/level/clusters.txt", "data/temp/", "data/index/frequency.txt");
	}
	
	public static void init(String features, int clusterNum, String clusters){
		Frequency.clusterNum = clusterNum;
		Frequency.clusters = clusters;
		Frequency.features = features;
	}
	
	public static void runJob (String features, int clusterNum, String clusters, String temp, String output){
		
		Frequency.init(features, clusterNum, clusters);
		getNames(features, temp + "/fn.txt");
		HadoopUtil.delete(output);
		runMR(temp + "/fn.txt", temp + "/freq");
		HadoopUtil.copyMerge(temp + "/freq", output);
		HadoopUtil.delete(temp + "/freq");
		
	}
	
	// read a folder of images from HDFS and store their names into a file on HDFS
	public static void getNames(String features, String file){
        try{
            FileSystem fs = FileSystem.get(new Configuration()); // open the image folder
            FileStatus[] status = fs.listStatus(new Path(features)); // get the list of images
            FSDataOutputStream out = fs.create(new Path(file)); // create output stream
            PrintWriter pw = new PrintWriter(out.getWrappedStream()); // create writer
            for (int i=0;i<status.length;i++){
            		//System.out.println(status[i].getPath().getName());
            		pw.println(status[i].getPath().getName());
            		//pw.flush();
            }
            pw.close();
            out.close();
            fs.close();
            System.out.println("feature filenames extraction is done");
        }catch(Exception e){
            System.out.println("File not found");
        }
	}
	
	public static void runMR(String infile, String outfile){

		JobConf conf = new JobConf(Frequency.class);
		conf.setJobName("Frequency");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(FEMap.class);
		conf.setReducerClass(FEReduce.class);
		conf.setNumReduceTasks(1);
		
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
	
	public static class FEMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			double[][] cs = readClusters(Frequency.clusters);//read clusters
			int[] marks = new int[Frequency.clusterNum];
			// read the features and 
			String file = value.toString();
			
			String data = Frequency.features + file;
			//read the file line by line
			Path dp = new Path(data);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream input = fs.open(dp);
			String line;
			while((line = input.readLine()) != null){
				double[] feature = new double[Frequency.featureSize];
				String[] args = line.split(" ");
				for (int i = 0; i < Frequency.featureSize; i++)
					feature[i] = Double.parseDouble(args[i + 4]);
				int index = findBestCluster(feature, cs);
				marks[index]++;
			}
			
			String result = "";
			int num = 0;
			for(int i = 0; i < Frequency.clusterNum; i++){
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
			double[][] cs = new double[Frequency.clusterNum][Frequency.featureSize];
			String line;
			for(int i = 0; i < Frequency.clusterNum; i ++){
				line = input.readLine();
				//System.out.println(line);
				String center = line.split("\\]")[0].split("c=\\[")[1];
				String[] array = center.split(", ");
				for(int j = 0; j < Frequency.featureSize; j++)
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