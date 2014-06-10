package ir.cluster;

import ir.util.HadoopUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
	
	public static void run(String[] args) {
		try {
			long ts0 = new Date().getTime();
			
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
			midLevelProcess(top, mid);
			//non-parallel bottom level clustering
			botLevelProcess(mid, bot, topK, botK, res);
			// merge the clusters into a single file
			merge(res, prefix);
			
			long ts1 = new Date().getTime();
			log("top-down clustering pipeline ends with total process time: " + (double)(ts1 - ts0) / (60 * 1000) + " min");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
	
	public static void merge(String src, String dst) throws IOException, InterruptedException{	
		// copy and merge files
		String temp = "temptemptemp";
		HadoopUtil.mkdir(temp);
		//gather all the clustered points in result directory and merge them.
		//those files should be in data/cluster/res/i/i/cluster-j-final/part-r-00000 (i=1,2,3,.....,j=1,2,...), need to get the exact paths of the files
		ArrayList<String> inputs=new ArrayList<String>();
		String[] res_folders=HadoopUtil.getListOfFolders(src);
		for (String res_folder:res_folders){
			String[] res_i_folders=HadoopUtil.getListOfFolders(res_folder);
			if(res_i_folders.length==1){
				res_i_folders=HadoopUtil.getListOfFolders(res_i_folders[0]);
			}
			for(String folder:res_i_folders){
				if(folder.endsWith("final")){
					inputs.add(folder);
				}
			}
		}
		String[] inputs_files=new String[inputs.size()];
		int index=0;
		for(String input:inputs){
			inputs_files[index++]=input+"/part-r-00000";
			System.out.println(inputs_files[index-1]);
		}
		
		ClusterDump.run_clusterdump(inputs_files, temp);
		HadoopUtil.cpFile(temp+"/part-00000", dst+"/clusters.txt");
		HadoopUtil.delete(temp);
	}
	
	public static String[] getFolders(String mid){
		
		//String[] folders = new String[topK];
		String[] tmp_folders=HadoopUtil.getListOfFolders(mid);
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
