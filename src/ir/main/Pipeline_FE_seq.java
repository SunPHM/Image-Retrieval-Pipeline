package ir.main;

import ir.cluster.VWDriver;
import ir.feature.FeatureExtraction;
import ir.feature.FeatureExtraction_byte;
import ir.feature.FeatureExtraction_seq;
import ir.index.Search;
import ir.util.HadoopUtil;
import ir.util.MeasureContainers;
import ir.util.RecordTime;

import java.io.IOException;
import java.util.Date;

public class Pipeline_FE_seq{

	// the main entry point for the Pipeline execution
	/** Setup
	 * @throws NumberFormatException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException
	 * 
	 * @Java: 1.6
	 * @Hadoop: 2.2.0
	 * @Mahout: 0.8
	 * @Solr: 4.6.1
	 */
	
	public static void main(String[] args) 
			throws NumberFormatException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
		//args[0]: the path to the images on HDFS or local file system
		//args[1]: the path of the output on HDFS or local file system
		//args[2]: the number of top-level clusters
		//args[3]: the number of bot-level clusters
		//args[4]=0|1|2, the botlevel clustering method to choose, 0: serial; 1: map-reduce, 2:  multi-thread
		//test arguments: data/images/ test/ 10 10 1
		 run(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]),Integer.parseInt(args[4]));
	}
	
	public static String run(String src, String dst, int topK, int botK, int botlvlcluster_type) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		long N = 1000 * 60;
		long startTime = new Date().getTime();
//		HadoopUtil.delete(dst);
		
		RecordTime rt=new RecordTime("recordtime.txt");
		rt.writeMsg("#Task: "+src+" "+dst+" "+topK+" "+botK+" "+botlvlcluster_type);
		//run the process to issue "hadoop job -list" and get the results in a file
		
		 MeasureContainers mt=new  MeasureContainers("recordcontainers.txt");
		 mt.start();
		 //Date date=new Date();
		 rt.writeMsg("$FEStart$ "+new Date().getTime());
		
		//TODO: call the main entry point of the Feature Extraction
		System.out.println("\n\nFeature Extraction");
		String features = dst + "/data/features";// the feature folder
		FeatureExtraction_seq.extractFeatures(src, features);
		System.out.println("Features folder:" + features);

		rt.writeMsg("$FEEnd$ "+new Date().getTime());
		long EndTime1 = new Date().getTime();
		
		
		rt.writeMsg("$VWStart$ "+new Date().getTime());
		
		//vocabulary construction and frequency generation
		System.out.println("\n\nvocabulary construction and frequency generation");
		String[] args = {features, dst, "" + topK, "" + botK};
		//!!
		String s = VWDriver.run(args, botlvlcluster_type,1);
		rt.writeMsg("$VWEnd$ "+new Date().getTime());
		long EndTime2 = new Date().getTime();
		
		rt.writeMsg("$ISStart$ "+new Date().getTime());
		//Indexing and Searching
		System.out.println("\n\n\n\n\nIndexing and Searching");
		//before run indexing, need to copy the frequency.txt file to local filesystem(index part reads from localfilesystem)---done 
		int clusterNum = topK * botK;
		
		Search.init(dst + "/data/frequency.txt", clusterNum, dst + "/cluster/clusters.txt", topK, botK);
		Search.runIndexing(dst + "/data/frequency.txt");
		
	//	Search.init(dst + "/data/frequency_new.txt", clusterNum, dst + "/cluster/clusters", topK, botK);
	//	Search.runIndexing(dst + "/data/frequency_new.txt");
		
		long EndTime3 = new Date().getTime();
		//to test or evaluate here
		String[] results = Search.search("data/images/all_souls_000001.jpg");
		
		for(String result : results){
			System.out.println(result);
		}
		
		long EndTime4 = new Date().getTime();
		
		rt.writeMsg("$ISEnd$ " + new Date().getTime());
		
		System.out.println("\n\n*******************************************  Running Time in minutes ********************************************");
		System.out.println("Total Running Time: "+ (double)(EndTime3 - startTime) / N 
				+"\nFeature Extraction: "+ (double)(EndTime1 - startTime) / N
			+"\nVWDriver: "+ (double)(EndTime2 - EndTime1) / N + "\n" + s
				+"Indexing: "+ (double)(EndTime3 - EndTime2) / N * 60 + " seconds\n" +
				"Searching: " + (double)(EndTime4 - EndTime3) / N * 60 + " seconds");

		String string_result="Total Running Time: "+ (double)(EndTime3 - startTime) / N 
				+"\nFeature Extraction: "+ (double)(EndTime1 - startTime) / N
			+"\nVWDriver: "+ (double)(EndTime2 - EndTime1) / N + "\n" + s
				+"Indexing: "+ (double)(EndTime3 - EndTime2) / N * 60 + " seconds\n" +
				"Searching: " + (double)(EndTime4 - EndTime3) / N * 60 + " seconds";
		rt.writeMsg(string_result);
		mt.stopMe();

		return string_result;
	}
}
