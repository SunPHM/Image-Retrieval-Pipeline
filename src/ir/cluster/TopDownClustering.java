package ir.cluster;

import ir.util.HadoopUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


public class TopDownClustering {
	
	private static double delta = 0.001;
	private static int x = 100;
	
	private static int topK = 0;
	private static int botK = 0;
	
	private static int maxIterations = 100;
	private static final CosineDistanceMeasure distance_measure = new CosineDistanceMeasure();
	
	//example args: data/cluster/fs.seq data/cluster/level  10 10
	public static void main(String[] args){
		run(args, 1);
	}
	
	public static String run(String[] args, int botlvlcluster_type) {
		
		long ts0 = 0, ts1 = 0, ts2 = 0, ts3 = 0;
		try {
			ts0 = new Date().getTime();
			// parse parameters
			String data = args[0];
			String prefix = args[1];
			String top = prefix + "/top";
			String mid = prefix + "/mid";
			String bot = prefix + "/bot";
			topK = Integer.parseInt(args[2]);
			botK = Integer.parseInt(args[3]);
			
			// execute top-down clustering pipeline
			HadoopUtil.delete(prefix);
			// top-level clustering
			topLevelProcess(data, top + "/cls", top, topK);
			ts1 = new Date().getTime();
			// medium processing between top-level and bottom-level clustering
			midLevelProcess(top, mid);
			//bottom level clustering
			ts2 = new Date().getTime();
			if(botlvlcluster_type == 0) botLevelProcess_Serial(mid, bot, topK, botK);
			else if(botlvlcluster_type == 1) botLevelProcess_MRJob(mid, bot, topK, botK);
			else if(botlvlcluster_type == 2) botLevelProcess_MultiThread(mid, bot, topK, botK);
			// merge the clusters into a single file
			merge(bot, prefix);
			ts3 = new Date().getTime();
			
			log("top-down clustering pipeline ends with total process time: " + (double)(ts3 - ts0) / (60 * 1000) + " min");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return "top-level clustering time = " +  (double)(ts1 - ts0) / (60 * 1000) + "\n" +
		"mid-level processing time = " + (double)(ts2 - ts1) / (60 * 1000) + "\n" +
		"bot-level clustering time = " + (double)(ts3 - ts2) / (60 * 1000) + "\n";
	}
	
	public static void topLevelProcess(String input, String cls, String top, int topK) {
		kmeans(input, cls, top, topK, delta, x);
	}
	
	public static void midLevelProcess(String top, String mid) throws IOException, InterruptedException {
		ClusterPP.run_clusterpp(top + "/clusteredPoints", mid);
	}
	
	public static void botLevelProcess_Serial(String mid, String bot, int topK, int botK) {
		String[] folders = getFolders(mid);
		
		for(int i = 0; i < folders.length; i++){
			// run mahout k-means clustering
			String input = folders[i] + "/part-m-0";
			String clusters = bot + "/" + i + "/cls";
			String output = bot + "/" + i;
			int k = botK;
			double cd = delta;
			kmeans(input,clusters,output,k,cd,x);
			
			// copy clusters to the result folder--no longer need it
			
			log("botlevel mahout kmeans clustering " + i + "> ends");
		}
	}
	
	
	public static void botLevelProcess_MRJob(String mid, String bot, int topK, int botK) {
		
		String[] folders = getFolders(mid);
		String output_folder = bot + "/temp";
		HadoopUtil.delete(output_folder);
		for(int i = 0; i < folders.length; i++){
			String input = folders[i] + "/part-m-0";
			String clusters = bot + "/" + i + "/cls";
			String output = bot + "/" + i;
			int k = botK;
			double cd = delta;
			writeBotLevelParameters("" + i, input + " " + clusters + " " + output + " " + k + " " + cd + " " + x, output_folder + "/" + i + ".txt");
		}
		runBotLevelClustering.run(output_folder, bot + "/whatever");
	}
	
	public static void writeBotLevelParameters(String key, String value, String filename){
		try{
			Path output_path = new Path(filename);	 
			Configuration conf = new Configuration();
			SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, output_path, Text.class, Text.class);
			writer.append(new Text(key), new Text(value));
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	public static void botLevelProcess_MultiThread(String mid, String bot, int topK, int botK) {
		String[] folders = getFolders(mid);
		KmeansThread[] threads=new KmeansThread[folders.length];
		for(int i = 0; i < folders.length; i++){
			String input=folders[i] + "/part-m-0";
			String clusters=bot + "/" + i + "/cls";
			String output=bot + "/" + i;
			int k = botK;
			double cd = delta;
			threads[i]=new KmeansThread(i, input, clusters,output,k,cd,x);
			threads[i].start();
		}
		
		//wait for all threads to complete
		for(int i = 0; i < folders.length; i++){
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("\n\n\n\n\n\nAttention, botlevelThread "+i+" illegal exceptions!!!!!\n*********\n******\n*********");
				e.printStackTrace();
			}
		}
	}
	
	public static void merge(String src, String dst) throws IOException, InterruptedException{	
		// copy and merge files
		String temp = dst + "/dumptemp";
		//gather all the clustered points in result directory and merge them.
		//those files should be in test/cluster/bot/i/cluster-j-final/* and not start with '_'
		ArrayList<String> inputs = new ArrayList<String>();
		String[] bot_folders = HadoopUtil.getListOfFolders(src);
		for (String bot_folder : bot_folders){
			String[] bot_i_folders = HadoopUtil.getListOfFolders(bot_folder);
			if(bot_i_folders.length == 1 && bot_i_folders[0].endsWith("final") == false)
				bot_i_folders = HadoopUtil.getListOfFolders(bot_i_folders[0]);
			for(String folder : bot_i_folders)
				if(folder.endsWith("final")) inputs.add(folder);
		}
		ArrayList<String> inputs_files = new ArrayList<String>();
		for(String input : inputs){
			String[] files = HadoopUtil.getListOfFiles(input);
			for(String file:files)
				if(file.startsWith("_") == false) inputs_files.add(file);
		}
		for(String file : inputs_files){
			System.out.println("files to merge: " + file);
		}
		
		String[] inputs_clusterdump = new String[inputs_files.size()];
		inputs_clusterdump = inputs_files.toArray(inputs_clusterdump);
		ClusterDump.run_clusterdump(inputs_clusterdump, temp);
		HadoopUtil.copyMerge(temp, dst + "/clusters.txt");
		
	}
	
	public static String[] getFolders(String mid){
		String[] tmp_folders=HadoopUtil.getListOfFolders(mid);
		System.out.println("In path:"+ mid);
		for(String str:tmp_folders)
			System.out.println("folder: "+str);
		if(tmp_folders == null || tmp_folders.length != topK){
			System.out.println("Error: number of folders in dir:  " + mid + "   does not equal to " + topK + ", please check!!!");
			return tmp_folders;
		}
		else return tmp_folders;
	}
	
	public static void kmeans(String input, String clusters, String output, int k, double cd, int x) {
		org.apache.hadoop.conf.Configuration conf = new Configuration();
		try {
			Path input_path = new Path(input);
			Path clusters_path = new Path(clusters + "/part-r-00000");
			Path output_path = new Path(output);
			HadoopUtil.delete(output);
			double clusterClassificationThreshold = 0; 
			 
			//read first K points from input folder as initial K clusters
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), input_path, conf);
			WritableComparable key = (WritableComparable)reader.getKeyClass().newInstance();
			VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
			SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, clusters_path, Text.class,Kluster.class);
			for (int i = 0; i < k; i++){
				reader.next(key, value);	 
				Vector vec = value.get();
				Kluster cluster = new Kluster(vec, i, distance_measure);
				writer.append(new Text(cluster.getIdentifier()), cluster);
			}
			reader.close(); writer.close();
			//System.out.println("initial " + k + "  clusters written to file: " + clusters);		
//*old 0.8api		
			KMeansDriver.run(conf, input_path, clusters_path, output_path, 
					distance_measure, cd, maxIterations, true, clusterClassificationThreshold, false);
//0.9 api
/*			KMeansDriver.run(conf, input_path, clusters_path, output_path, 
					cd, x, true, clusterClassificationThreshold, false);
					*/
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void log(String msg){
		Date now = new Date();
		System.out.println((now.getYear() - 100) + "/" + (now.getMonth() + 1) + "/" + now.getDate() + " " +  
				now.getHours() + ":" + now.getMinutes() + ":" + now.getSeconds() + ": " + msg);
	}
	
}

class runBotLevelClustering{
	public static void run(String input, String whatever_output){
		//HadoopUtil.delete(output);
		System.out.println("botlevel clustering using MR job parallelism!!!!!!!");

		JobConf conf = new JobConf(runBotLevelClustering.class);
		conf.setJobName("botlevelclustering");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(ClusterWritable.class);

		conf.setMapperClass(runBotLevelClustering.KmeansMap.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(whatever_output));

		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("botlevel clustering is done");
	}
	
	public static class KmeansMap extends MapReduceBase implements Mapper<Text, Text, IntWritable,  ClusterWritable> {
		@Override
		public void map(Text key, Text value, OutputCollector<IntWritable,  ClusterWritable> output, Reporter reporter) 
				throws IOException {
			String args=value.toString();
			//String input, String clusters, String output, int k, double cd, int x
			String[] splits=args.split(" ");
			TopDownClustering.kmeans(splits[0], splits[1], splits[2], Integer.parseInt(splits[3]), Double.parseDouble(splits[4]), Integer.parseInt(splits[5]));
			TopDownClustering.log("bottom-level clustering " + key.toString() + " ends");
		}
	}
}

class KmeansThread extends Thread
{
	int id;
	String input;
	String clusters;
	String output;
	int k;
	double cd;
	int x;

   public KmeansThread(int id, String input, String clusters, String output, int k, double cd, int x)
   {
	  this.id = id;
      this.input=input;
      this.clusters=clusters;
      this.output=output;
      this.k=k;
      this.cd=cd;
      this.x=x;
   }

   @Override
   public void run()
   {
      TopDownClustering.kmeans(input, clusters, output, k, cd, x);
      TopDownClustering.log("thread " + id + " ends");
   }
}
