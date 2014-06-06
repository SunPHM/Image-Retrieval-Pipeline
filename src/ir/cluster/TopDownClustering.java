package ir.cluster;

import ir.util.HadoopUtil;

import java.io.File;
import java.io.IOException;
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
	
	private static String dm = "org.apache.mahout.common.distance.CosineDistanceMeasure";
	private static double delta = 0.001;
	private static int x = 100;
	
	private static int topK = 0;
	private static int botK = 0;
	
	private static int maxIterations=100;
	private static final CosineDistanceMeasure distance_measure=new CosineDistanceMeasure();
	
	//sample args: data/cluster/fs.seq data/cluster/level  10 10
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException{
		run(args);
	}
	
	public static void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException{
		
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
		// non-parallel bottom level clustering
		botLevelProcess(mid, bot, topK, botK, res);
		// merge the clusters into a single file
		merge(res, prefix);
		
		long ts1 = new Date().getTime();
		log("top-down clustering pipeline ends with total process time: " + (double)(ts1 - ts0) / (60 * 1000) + " min");
	}
	
	public static void topLevelProcess(String input, String cls, String top, int topK) {
		kmeans(input, cls, top, topK, delta, x);
	}
	
	public static void midLevelProcess(String top, String mid) throws IOException, InterruptedException {
		String command = "mahout clusterpp -i " + top + " -o " + mid + " -xm sequential";
		log(command);
		run(command);
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
			
			//get the name of the final folder
			File folder = new File(src);
			File[] listOfFiles = folder.listFiles();

			for (int j = 0; j < listOfFiles.length; j++) {
				if (listOfFiles[j].isDirectory()) {
			        System.out.println("Directory " + listOfFiles[j].getPath());
			        if(listOfFiles[j].getName().endsWith("final")){
			        	src=src+"/"+listOfFiles[j].getName();
			        }
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
	
		///////////////how to change this
		String cmd1 = "mahout clusterdump -i " + src + "/*/" + " -o " + temp + "/clusters.txt";
		log(cmd1);
		run(cmd1);
		
		HadoopUtil.cpFile(temp+"/clusters.txt", dst+"/clusters.txt");
		/*	
		String cmd3 = "rm -r " + temp;
		log(cmd3);
		run(cmd3);
		 */
		HadoopUtil.delete(temp);
		//HadoopUtil.delete(dst + "/tmp/tmp.txt");
	}
	
	public static String[] getFolders(String mid){
		
		String[] folders = new String[topK];
		File folder = new File(mid);
		File[] listOfFiles = folder.listFiles();

		    for (int i = 0; i < listOfFiles.length; i++) {
		      if (listOfFiles[i].isFile()) {
		        System.out.println("File " + listOfFiles[i].getName());
		        //do nothing
		      } else if (listOfFiles[i].isDirectory()) {
		        System.out.println("Directory " + listOfFiles[i].getPath());
		        //
		        folders[i]=listOfFiles[i].getPath();
		      }
		    }
		return folders;
	}
	
	public static void kmeans(String input, String clusters, String output, int k, double cd, int x) {
		
		org.apache.hadoop.conf.Configuration conf=new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			Path input_path = new Path(input);
			Path clusters_path=new Path(clusters+"/part-r-00000");
			Path output_path=new Path(output);
			HadoopUtil.delete(output);
			double clusterClassificationThreshold=0;//////???	 
			 
			//read first K points from input folder as initial K clusters
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), input_path, conf);
			WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
			VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
			SequenceFile.Writer writer=new SequenceFile.Writer(FileSystem.get(conf), conf, clusters_path, Text.class,Kluster.class);
			for (int i=0;i<k;i++){
				reader.next(key, value);
				 
				Vector vec =value.get();
				Kluster cluster=new Kluster(vec,i,distance_measure );
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
	
	
	public static void copyResults(String cs, String res) throws IOException, InterruptedException{
		String cmd0 = "hadoop fs -mkdir " + res;
		run(cmd0);
		
		String cmd = "hadoop fs -cp " + cs + " " + res;
		log(cmd);
		run(cmd);
	}
	
	public static void log(String msg){
		Date now = new Date();
		System.out.println((now.getYear() - 100) + "/" + (now.getMonth() + 1) + "/" + now.getDate() + " " +  
				now.getHours() + ":" + now.getMinutes() + ":" + now.getSeconds() + ": " + msg);
	}
	
	public static void run(String command) throws IOException, InterruptedException{
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec(command);
	    p.waitFor();
	}
}
