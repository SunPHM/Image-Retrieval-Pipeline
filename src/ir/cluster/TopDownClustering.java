package ir.cluster;

import ir.util.HadoopUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.Vector;

public class TopDownClustering {
	
	private static String dm = "org.apache.mahout.common.distance.CosineDistanceMeasure";
	private static double delta = 0.001;
	private static int x = 100;
	
	private static int topK = 0;
	private static int botK = 0;
	
	private static int maxIterations=100;
	private static final CosineDistanceMeasure distance_measure=new CosineDistanceMeasure();
	
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
		//clean(prefix);
		topLevelProcess(data, top + "/cls", top, topK);
		//midLevelProcess(top, mid);
		// parallel bottom level clustering
		//botLevelProcess(mid, bot, topK, botK, res);
		// merge the clusters into a single file
		//merge(res, prefix);
		
		long ts1 = new Date().getTime();
		log("top-down clustering pipeline ends with total process time: " + (double)(ts1 - ts0) / (60 * 1000) + " min");
	}
	
	public static void clean(String prefix) throws IOException, InterruptedException{
		String cmd = "hadoop fs -rm -R " + prefix;
		run(cmd);
	}
	
	public static void topLevelProcess(String input, String cls, String top, int topK) throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException{
		kmeans(input, cls, top, topK, delta, x);
	}
	
	public static void midLevelProcess(String top, String mid) throws IOException, InterruptedException{
		String command = "mahout clusterpp -i " + top + " -o " + mid + " -xm sequential";
		log(command);
		run(command);
	}
	
	public static void botLevelProcess(String mid, String bot, int topK, int botK, String res) throws IOException, InterruptedException{
		String[] folders = getFolders(mid);
		String cmd = "hadoop fs -mkdir " + res;
		run(cmd);
		
		//Thread[] ts = new Thread[topK];
		
		for(int i = 0; i < folders.length; i++){
			
			//ts[i] = parrelBotProcess(folders[i] + "/part-m-0", 	bot + "/" + i + "/cls", 	bot + "/" + i, 	botK, 	delta, 	x, 
			//		bot + "/" + i + "/clusters-*-final/*", 	res + "/" + i, 	i);
			//change to serially processing
			
			//parrelBotProcess(String input, String clusters, String output, int k, double cd, int x, 
			//			String src, String dst, int num)
			String input=folders[i] + "/part-m-0";
			String clusters=bot + "/" + i + "/cls";
			String output=bot + "/" + i;
			int k=botK;
			double cd = delta;
			//int x=x;
			
			String src=bot + "/" + i + "/clusters-*-final/*";
			String dst=res + "/" + i;
			int num=i;
			
			String cmd0 = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
					+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl";
			//String cmd0 = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
			//		+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl -xm sequential";
			String cmd1 = "hadoop fs -mkdir " + dst;
			String cmd2 = "hadoop fs -cp " + src + " " + dst;
			//String cmd = cmd0 + "\n\r" + cmd1 + "\n\r" + cmd2;
			
			//Thread t = new Thread(new BotThread(cmd0, cmd1, cmd2, num));
			//t.start();
			
			log(cmd0);
			run(cmd0);
			log(cmd1);
			run(cmd1);
			log(cmd2);
			run(cmd2);
			log("botlevel clustering " + num + "> ends");
			
		}
		
		//for(int i = 0; i < ts.length; i++){
		//	ts[i].join();
		//}
	}
	
	public static void merge(String src, String dst) throws IOException, InterruptedException{
		// copy and merge files
		// HadoopUtil.copyMerge(src, dst + "/tmp/tmp.txt");
		// mahout clusterdump
		String temp = "temptemptemp";
		String cmd0 = "mkdir " + temp;
		log(cmd0);
		run(cmd0);
		String cmd1 = "mahout clusterdump -i " + src + "/*/" + " -o " + temp + "/clusters.txt";
		log(cmd1);
		run(cmd1);
		String cmd2 = "hadoop fs -put " + temp + "/clusters.txt" + " " + dst;
		log(cmd2);
		run(cmd2);
		String cmd3 = "rm -r " + temp;
		log(cmd3);
		run(cmd3);
		//HadoopUtil.delete(dst + "/tmp/tmp.txt");
	}
	
	public static String[] getFolders(String mid) throws IOException, InterruptedException{
		
		String[] folders = new String[topK];
		int i = -1;
		String cmd = "hadoop fs -ls " + mid;
		log(cmd);
		
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec(cmd);
		String line;
		BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()) );
	    while ((line = in.readLine()) != null) {
	    	if(i == -1) i = 0;
	    	else{
	    		folders[i] = line.split(" +")[7];
	    		i++;
	    	}
	    }
	    in.close();
		p.waitFor();
		return folders;
	}
	
	public static void kmeans(String input, String clusters, String output, int k, double cd, int x) 
			throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException{
		String command = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
				+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl -xm sequential"  ;
		org.apache.hadoop.conf.Configuration conf=new Configuration();
		FileSystem fs = FileSystem.get(conf);
		 Path input_path = new Path(input);
		 Path clusters_path=new Path(clusters);
		 Path output_path=new Path(output);
		 HadoopUtil.delete(output);
		 double clusterClassificationThreshold=0;//////???	 
		 
		 //read first K points from input folder as initial K clusters
		 SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), input_path, conf);
		 WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
		 Writable value = (Writable) reader.getValueClass().newInstance();
		 SequenceFile.Writer writer=new SequenceFile.Writer(FileSystem.get(conf), conf, clusters_path, Text.class,Kluster.class);
		 for (int i=0;i<k;i++){
			 reader.next(key, value);
			 Vector vec =new Vector(value);
			 Kluster cluster=new Kluster(,i,distance_measure );
			 writer.append(new Text(cluster.getIdentifier()), cluster);
		 }
		 reader.close();writer.close();
/* public static void run(org.apache.hadoop.conf.Configuration conf,
                       org.apache.hadoop.fs.Path input,
                       org.apache.hadoop.fs.Path clustersIn,
                       org.apache.hadoop.fs.Path output,
                       double convergenceDelta,
                       int maxIterations,
                       boolean runClustering,
                       double clusterClassificationThreshold,
                       boolean runSequential)
                throws IOException,
                       InterruptedException,
                       ClassNotFoundException
			Iterate over the input vectors to produce clusters and, if requested, use the results of the final iteration to cluster the input vectors.
					Parameters:
						input - the directory pathname for input points
						clustersIn - the directory pathname for initial & computed clusters
						output - the directory pathname for output points
						convergenceDelta - the convergence delta value
						maxIterations - the maximum number of iterations
						runClustering - true if points are to be clustered after iterations are completed
						clusterClassificationThreshold - Is a clustering strictness / outlier removal parameter. Its value should be between 0 and 1. Vectors having pdf below this value will not be clustered.
						runSequential - if true execute sequential algorithm
 */
		KMeansDriver.run(conf, input_path, clusters_path, output_path, 
				distance_measure ,
				 cd,  maxIterations, true, clusterClassificationThreshold, false);
		
		
	//	log(command);
	//	run(command);
	}
	
	/*public static Thread parrelBotProcess(String input, String clusters, String output, int k, double cd, int x, String src, String dst, int num) throws IOException, InterruptedException{
		
		String cmd0 = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
				+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl";
		//String cmd0 = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
		//		+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl -xm sequential";
		String cmd1 = "hadoop fs -mkdir " + dst;
		String cmd2 = "hadoop fs -cp " + src + " " + dst;
		//String cmd = cmd0 + "\n\r" + cmd1 + "\n\r" + cmd2;
		
		Thread t = new Thread(new BotThread(cmd0, cmd1, cmd2, num));
		t.start();
		return t;
	}
	*/
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
