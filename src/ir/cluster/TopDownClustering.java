package ir.cluster;

import ir.util.HadoopUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

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
		run(args);
	}
	
	public static String run(String[] args) {
		
		long ts0 = 0, ts1 = 0, ts2 = 0, ts3 = 0;
		try {
			ts0 = new Date().getTime();
			
			// parse parameters
			String data = args[0];
			String prefix = args[1];
			String top = prefix + "/top";
			String mid = prefix + "/mid";
			String bot = prefix + "/bot";
			String res = prefix + "/res";
			topK = Integer.parseInt(args[2]);
			botK = Integer.parseInt(args[3]);
			
			// execute pipeline
			HadoopUtil.delete(prefix);
			topLevelProcess(data, top + "/cls", top, topK);
			ts1 = new Date().getTime();
			midLevelProcess(top, mid);
			//non-parallel bottom level clustering
			ts2 = new Date().getTime();
			botLevelProcess_Parrallel(mid, bot, topK, botK, res);
			// merge the clusters into a single file
			merge(res, prefix);
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
		ClusterPP.run_clusterpp(top+"/clusteredPoints", mid);
	}
	
	public static void botLevelProcess(String mid, String bot, int topK, int botK, String res) {
		
		String[] folders = getFolders(mid);
		HadoopUtil.mkdir(res);
		
		for(int i = 0; i < folders.length; i++){
			String input=folders[i] + "/part-m-0";
			String clusters=bot + "/" + i + "/cls";
			String output=bot + "/" + i;
			int k = botK;
			double cd = delta;
			int num=i;

			kmeans(input,clusters,output,k,cd,x);
			
			String src=bot + "/" + i ;//+ "/clusters-*-final/*";
			String dst=res + "/" + i;
			
			//get the name of the final folder -- dont know which i in clusters-i-final
			String[] listOfFiles = HadoopUtil.getListOfFiles(src);

			for (int j = 0; j < listOfFiles.length; j++) {
			       // System.out.println("Directory " + listOfFiles[j].getPath());
			    if(listOfFiles[j].endsWith("final")){
			        	src=src+"/"+listOfFiles[j];
			    }
				
			}
			
			HadoopUtil.mkdir(dst);
			HadoopUtil.cpdir(src, dst);
			
			log("botlevel clustering " + num + "> ends");
			
		}
	}
	
	
	
	public static void botLevelProcess_Parrallel(String mid, String bot, int topK, int botK, String res) {
		
		String[] folders = getFolders(mid);
		HadoopUtil.mkdir(res);
		
		org.apache.hadoop.conf.Configuration conf=new Configuration();
		String output_string=bot+"/temp";
		Path output_path = new Path(output_string);
//		HadoopUtil.delete(output);
		double clusterClassificationThreshold = 0;//////???	 
		SequenceFile.Writer writer=null;
		try {
			writer = new SequenceFile.Writer(FileSystem.get(conf), conf, output_path, Text.class,Text.class);
			for(int i = 0; i < folders.length; i++){
				String input=folders[i] + "/part-m-0";
				String clusters=bot + "/" + i + "/cls";
				String output=bot + "/" + i;
				int k = botK;
				double cd = delta;
	
				//kmeans(input,clusters,output,k,cd,x);
				//write the args to file. run kmeans in mappers
				writer.append(new Text(""+i), new Text(input+" "+clusters+" "+output+" "+k+" "+cd+" "+x));
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		runBotLevelClustering.run(output_string,bot+"/whatever");
		for(int i = 0; i < folders.length; i++){
			String src=bot + "/" + i ;//+ "/clusters-*-final/*";
			String dst=res + "/" + i;
			
			//get the name of the final folder -- dont know which i in clusters-i-final
			String[] listOfFiles = HadoopUtil.getListOfFolders(src);

			for (int j = 0; j < listOfFiles.length; j++) {
			       // System.out.println("Directory " + listOfFiles[j].getPath());
			    if(listOfFiles[j].endsWith("final")){
			        	src=listOfFiles[j];
			    }
				
			}
			
			HadoopUtil.mkdir(dst);
			HadoopUtil.cpdir(src, dst);
			log("botlevel clustering " +  i+ "> ends");
		}
		
		
		
	}
	
	
	public static void merge(String src, String dst) throws IOException, InterruptedException{	
		// copy and merge files
		String temp = "temptemptemp";
		HadoopUtil.mkdir(temp);
		//gather all the clustered points in result directory and merge them.
		//those files should be in data/cluster/res/i/i/cluster-j-final/*() (i=1,2,3,.....,j=1,2,...), need to get the exact paths of the files
		ArrayList<String> inputs=new ArrayList<String>();
		String[] res_folders=HadoopUtil.getListOfFolders(src);
		for (String res_folder:res_folders){
			String[] res_i_folders=HadoopUtil.getListOfFolders(res_folder);
			if(res_i_folders.length==1&&res_i_folders[0].endsWith("final")==false){
				res_i_folders=HadoopUtil.getListOfFolders(res_i_folders[0]);
			}
			for(String folder:res_i_folders){
				if(folder.endsWith("final")){
					inputs.add(folder);
				}
			}
		}
		ArrayList<String> inputs_files=new ArrayList<String>();
		int index=0;
		for(String input:inputs){
			//inputs_files[index++]=input+"/part-r-00000";
			String[] files=HadoopUtil.getListOfFiles(input);
			for(String file:files){
				if(file.startsWith("_")==false){
						inputs_files.add(file);
						}
			}
			
		}
		for(String file:inputs_files){
			System.out.println("files to merge: "+file);
		}
		String [] inputs_clusterdump=new String[inputs_files.size()];
		inputs_clusterdump=inputs_files.toArray(inputs_clusterdump);
		ClusterDump.run_clusterdump(inputs_clusterdump, temp);
		
		//HadoopUtil.cpFile(temp+"/part-00000", dst+"/clusters.txt");
		HadoopUtil.copyMerge(temp, dst+"/clusters.txt");
		HadoopUtil.delete(temp);
	}
	
	public static String[] getFolders(String mid){
		
		//String[] folders = new String[topK];
		String[] tmp_folders=HadoopUtil.getListOfFolders(mid);
		//debug
		System.out.println("In path:"+ mid);
		for(String str:tmp_folders)
		{
			System.out.println("folder: "+str);
		}
		//enddebug
		
		if(tmp_folders==null||tmp_folders.length!=topK){
			System.out.println("Error: number of folders in dir:  " + mid + "   does not equal to " + topK + ", please check!!!");
			return tmp_folders;
		}
		else{
			return tmp_folders;
		}
		
		//return folders;
	}
	
	public static void kmeans(String input, String clusters, String output, int k, double cd, int x) {
		
		org.apache.hadoop.conf.Configuration conf=new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			Path input_path = new Path(input);
			Path clusters_path = new Path(clusters+"/part-r-00000");
			Path output_path = new Path(output);
			HadoopUtil.delete(output);
			double clusterClassificationThreshold = 0;//////???	 
			 
			//read first K points from input folder as initial K clusters
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), input_path, conf);
			WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
			VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
			SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, clusters_path, Text.class,Kluster.class);
			for (int i = 0;i < k;i++){
				reader.next(key, value);
				 
				Vector vec = value.get();
				Kluster cluster = new Kluster(vec,i,distance_measure );
				writer.append(new Text(cluster.getIdentifier()), cluster);
			}
			reader.close();writer.close();
			System.out.println("initial "+k+"  clusters written to file: " + clusters);
			 
			KMeansDriver.run(conf, input_path, clusters_path, output_path, 
					distance_measure ,
					 cd,  maxIterations, true, clusterClassificationThreshold, false);
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
	
	
	
	public static void log(String msg){
		Date now = new Date();
		System.out.println((now.getYear() - 100) + "/" + (now.getMonth() + 1) + "/" + now.getDate() + " " +  
				now.getHours() + ":" + now.getMinutes() + ":" + now.getSeconds() + ": " + msg);
	}
	
}
class runBotLevelClustering{
	public static void run(String input,String whatever_output){
		//HadoopUtil.delete(output);

		JobConf conf = new JobConf(runBotLevelClustering.class);
		conf.setJobName("botlevelclustering");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(ClusterWritable.class);

		conf.setMapperClass(runBotLevelClustering.KmeansMap.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    conf.setNumReduceTasks(1);

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
		}

	}
}

