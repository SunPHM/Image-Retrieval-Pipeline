package ir.cluster;

import ir.util.HadoopUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.mahout.math.Arrays;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TopDownFrequency {
	
	//input: clusters/level/clusterid, features
	//output: a file containing both the name of file and the cluster id
	private static  boolean Use_Cosine_distance = true;
	public static int featureSize = 128;
	

	public static void main(String[] args) throws IOException{
		double[][] cs = readClusters("test/cluster/clusters/0/0.txt", 10);
		for(int c = 0; c < cs.length; c++){
			System.out.println(Arrays.toString(cs[c]));
		}
		//String features = "test_fe_seq2seq_100images/data/features.seq";
		//String clusters = "test_fe_seq2seq_100images/cluster/clusters";
		//String dividedfeatures = "test_fe_seq2seq_100images/data/dividedfeatures";
		//int topclusterNum = 10;
		//predivide(features, clusters + "/0/0.txt", dividedfeatures, topclusterNum);
		//runJob(features, botclusterNum, topclusterNum, clusters, temp, output);
	}
	
	/**Setup
	 * @param
	 * features folder
	 * clusters folder : containers layers of clusters.txt
	 * topclusterNum
	 * botclusterNum
	 * @throws IOException
	 * */
	public static void runJob (String features, int topclusterNum, int botclusterNum, String clusters, String temp, String output) 
			throws IOException{
		HadoopUtil.delete(temp);
		HadoopUtil.delete(output);
		
		String dividedfeatures = temp + "/dividedfeatures";
		predivide(features, clusters + "/0/0.txt", dividedfeatures, topclusterNum);
		
		String freqtemp = temp+"/fretemp";
		botlevelFrequencyJob(dividedfeatures, clusters + "/1/", topclusterNum, botclusterNum, freqtemp);
		HadoopUtil.copyMerge(freqtemp, output);
	}
	
	//pre-divide the features according to  the clusters from toplevel clustering
	// clusters : the top level clustering result; output : the divided features 0.seq, 1.seq ....
	public static void predivide(String features, String clusters, String output, int topclusterNum) throws IOException{
		HadoopUtil.delete(output);
		JobConf conf = new JobConf(TopDownFrequency.class);
		conf.setJobName("TopDownFrequency");
		// configure parameters
		conf.set("features", features);
		conf.set("clusterNum", new Integer(topclusterNum).toString());
		conf.set("clusters", clusters);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(TextVectorWritable.class);
		conf.setMapperClass(predivideMap.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(MultiFileOutput.class);
	    SequenceFileInputFormat.setInputPaths(conf, new Path(features));
	    MultiFileOutput.setOutputPath(conf, new Path(output));
		JobClient.runJob(conf);
	}
	
	public static class predivideMap extends MapReduceBase implements Mapper<Text, VectorWritable, Text,  TextVectorWritable> {

		public static int featureSize = 128;
		public static int clusterNum = 100;
		public static String clusters = "";
		public static double[][] cs = null;
		//private TextVectorWritable tvw = new TextVectorWritable();
		@Override
		public void configure(JobConf job) {
		   clusterNum = Integer.parseInt(job.get("clusterNum"));
		   clusters = job.get("clusters");
		   try {
			cs = readClusters(clusters, clusterNum);
		   } catch (IOException e) {
			   // TODO Auto-generated catch block
			   e.printStackTrace();
		   }
		}
		
		@Override
		public void map(Text key, VectorWritable value, OutputCollector<Text, TextVectorWritable> output, Reporter reporter) throws IOException {
			String file = key.toString();
			Vector vector = value.get();
			double[] feature = new double[featureSize];
			for (int i = 0; i < featureSize; i++)
				feature[i] = vector.get(i);
			int index = findBestCluster(feature, cs);
			TextVectorWritable tvw = new TextVectorWritable();
			tvw.set(file, vector);
			output.collect(new Text("" + index), tvw);
		}
	}
	
	//frequency job
	public static void botlevelFrequencyJob(String dividedfeatures, String botclusters, 
			int topclusterNum, int botclusterNum, String temp) {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(TopDownFrequency.class);
		conf.setJobName("TopDownFrequency");
		
		// configure parameters
		conf.set("features", dividedfeatures);
		conf.set("topclusterNum", "" + topclusterNum);
		conf.set("botclusterNum", "" + botclusterNum);
		conf.set("clusters", botclusters);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(FrequencyMap.class);
		conf.setReducerClass(FrequencyReduce.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    //delete _policy file and _success file
	    String[] files = HadoopUtil.getListOfFiles(dividedfeatures);
	    for (String file:files){
	    	if (file.startsWith("_")){
	    		HadoopUtil.delete(file);
	    	}
	    }
	    
		FileInputFormat.setInputPaths(conf, new Path(dividedfeatures));
		FileOutputFormat.setOutputPath(conf, new Path(temp));
		
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public static class FrequencyMap extends MapReduceBase implements Mapper<Text, TextVectorWritable, Text, Text> {

		public static int featureSize = 128;
		public static int topclusterId = 0;
		public static int botclusterNum = 10;
		public static String clusters = "";
		public static String features = "";
		public static double[][] cs = null;
		@Override
		public void configure(JobConf job) {
		   botclusterNum = Integer.parseInt(job.get("botclusterNum"));
		   clusters = job.get("clusters");
		   features = job.get("features");
		  
		   String filename = job.get("map.input.file");
		   String splits[] = filename.split("/");
		   String name = splits[splits.length -1];	
		   String topclusterId_str = name.split("\\.")[0];
		   topclusterId = Integer.parseInt(topclusterId_str);
		   String input_clusters_file = "" + topclusterId + ".txt";
		   //System.out.println(clusters);
		   try {
			cs = readClusters(clusters + "/" + input_clusters_file, botclusterNum);
		   } catch (IOException e) {
			   // TODO Auto-generated catch block
			   e.printStackTrace();
		   }//read clusters
		}
		
		@Override
		public void map(Text key, TextVectorWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String str_key = key.toString();
			Vector vector = value.getVectorWritable().get();
			double[] feature = new double[featureSize];
			for (int i = 0; i < featureSize; i++)
				feature[i] = vector.get(i);
			//find the toplevel cluster this feature belongs to
			int index = findBestCluster(feature, cs);
			if(Integer.parseInt(str_key) != topclusterId)
				throw new IOException("topclusterId inconsistancy : in map " + str_key + "\t in configure " + topclusterId);
				   
			int clusterId = topclusterId * botclusterNum + index;
			output.collect(value.getText(), new Text("" + clusterId));
		}
	}
	
	public static class FrequencyReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String sum = "";
			int total = 0;
			while (values.hasNext()) {
				String s = values.next().toString();
				if(!s.equals("")){
					if (sum.length() == 0) sum = s;
					else sum = sum + " " + s;
					total += 1;
				}
			}
			output.collect(key, new Text(total + "\t" + sum));
		}
	}
	
	//for each cluster in toplevelclustering generate a file
	static class MultiFileOutput extends MultipleSequenceFileOutputFormat<Text, TextVectorWritable> {
        protected String generateFileNameForKeyValue(Text key, TextVectorWritable value,String name) {
                return key.toString()+".seq";
        }
	}
	

	
	public static double[][] readClusters(String clusters, int clusterNum) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(clusters);
		FSDataInputStream input = fs.open(path);
		double[][] cs = new double[clusterNum][featureSize];
		String line;
		//System.out.println(clusters);
		while((line = input.readLine()) != null){
			//System.out.println("the start of processing one line");
			int i = Integer.parseInt(line.split("\t")[0]);
			//System.out.println("id = " + i);
			String center = line.split("\\}")[0].split("\\{")[1];
			//System.out.println(center);
			String[] array = center.split(",");
			//if normal case, correct format
			if(center.contains(":") == false) {
				for(int j = 0; j < featureSize; j++){
					String[] co = array[j].split(":"); // adding regex patterns to solve the wrong format problem
					cs[i][j] = Double.parseDouble(co[co.length - 1]);
				}
			}
			else { // abnormal case, fill those missing dimensions with zeros
				String[] result_array = new String[featureSize];
				for (int k = 0; k < featureSize; k ++){
					result_array[k] = "0";
				}
				for(String str : array){
					String[] splits = str.split(":");
					int index = Integer.parseInt(splits[0]);
					result_array[index] = splits[1];
				}
				for(int j = 0; j < featureSize; j++){
					cs[i][j] = Double.parseDouble(result_array[j]);
				}
			}
		}
		return cs;
	}
	
	public static int findBestCluster(double[] feature, double[][] clusters){
		if(KMeans.dmType == 0) Use_Cosine_distance = true; else Use_Cosine_distance = false;
		if (Use_Cosine_distance == true){
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
		else {
			int index = -1;
			double distance = Double.MAX_VALUE;
			//feature = norm(feature);
			for(int i = 0; i < clusters.length; i++){
				double ds = 0;
				for(int j = 0; j < clusters[i].length; j++){
					ds += (feature[j] - clusters[i][j]) * (feature[j] - clusters[i][j]);
				}
				ds = Math.sqrt(ds);
				
				if(ds < distance){
					distance = ds;
					index = i;
				}
			}
			return index;
		}
	}
}
