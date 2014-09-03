package ir.main;


import java.util.Date;

import ir.cluster.VWDriver;
import ir.feature.FeatureExtraction;
import ir.index.EvaluateImageNet;
import ir.index.Search;
import ir.util.HadoopUtil;
import ir.util.MeasureContainers;
import ir.util.RecordTime;

public class Pipeline_ImageNet_Evaluation {

	// the main entry point for the Pipeline execution
	/** Setup
	 * @return 
	 * @Java: 1.6
	 * @Hadoop: 1.2.1
	 * @Mahout: 0.8
	 * @Solr: 4.6.1
	 */
	
	public static void main(String[] args) {
		//args[0]: the path to the images on HDFS or local file system
		//args[1]: the path of the output on HDFS or local file system
		//args[2]: the number of top-level clusters
		//args[3]: the number of bot-level clusters
		//args[4]=0|1|2, the botlevel clustering method to choose, 0: serial; 1: MR job based, 2:  multi-thread
		//args[5]: the test folder containing images for test
		// test arguments: data/images/ test/ 10 10 1
		run(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]), args[5]);
	}
	
	public static String run(String src, String dst, int topK, int botK, int botlvlcluster_type, String tests){
		long N = 1000 * 60;
		long startTime = new Date().getTime();
		HadoopUtil.delete(dst);
		
		//record time of each phase to a file
		RecordTime rt = new RecordTime("recordtime.txt");
		rt.writeMsg("#Task: "+src+" "+dst+" "+topK+" "+botK+" "+botlvlcluster_type);
		//run the process to issue "hadoop job -list" and get the results in a file
		MeasureContainers mt = new  MeasureContainers("recordcontainers.txt");
		mt.start();
		
		//Feature Extraction
		rt.writeMsg("$FEStart$ "+new Date().getTime());
		System.out.println("\n\nFeature Extraction");
		String features = dst + "/data/features.seq";// the feature folder
		FeatureExtraction.extractFeatures(src, features, dst + "/temp/fe/");
		//FeatureExtractionSeqFile.extractFeatures(src, features, dst + "/temp/fe/");
		System.out.println("Features folder:" + features);
		rt.writeMsg("$FEEnd$ "+new Date().getTime());
		long EndTime1 = new Date().getTime();
		
		//Vocabulary Construction and Frequency Generation
		rt.writeMsg("$VWStart$ "+new Date().getTime());
		System.out.println("\n\nvocabulary construction and frequency generation");
		String[] args = {features, dst, "" + topK, "" + botK};
		String s = VWDriver.run(args, botlvlcluster_type);
		rt.writeMsg("$VWEnd$ "+new Date().getTime());
		long EndTime2 = new Date().getTime();
		
		//Indexing and Searching
		rt.writeMsg("$ISStart$ "+new Date().getTime());
		System.out.println("\n\nIndexing and Searching");
		//before run indexing, need to copy the frequency.txt file to local filesystem(index part reads from localfilesystem)---done 
		int clusterNum = topK * botK;
		Search.init(dst + "/data/frequency.txt", clusterNum, dst + "/cluster/clusters.txt");
		Search.runIndexing(dst + "/data/frequency.txt");
		long EndTime3 = new Date().getTime();
		//to test or evaluate here
		//Search.search(src + "/all_souls_000000.jpg");
		EvaluateImageNet.evaluate(tests);
		long EndTime4 = new Date().getTime();
		rt.writeMsg("$ISEnd$ " + new Date().getTime());
		 
		System.out.println("\n\n*******************************************  Running Time in minutes ********************************************");
		System.out.println("Total Running Time: "+ (double)(EndTime3 - startTime) / N 
				+"\nFeature Extraction: "+ (double)(EndTime1 - startTime) / N
			+"\nVVWDriver: "+ (double)(EndTime2 - EndTime1) / N + "\n" + s
				+"Indexing: "+ (double)(EndTime3 - EndTime2) / N * 60 + " seconds\n" +
				"Searching: " + (double)(EndTime4 - EndTime3) / N * 60 + " seconds");

		String string_result="Total Running Time: "+ (double)(EndTime3 - startTime) / N 
				+"\nFeature Extraction: "+ (double)(EndTime1 - startTime) / N
			+"\nVVWDriver: "+ (double)(EndTime2 - EndTime1) / N + "\n" + s
				+"Indexing: "+ (double)(EndTime3 - EndTime2) / N * 60 + " seconds\n" +
				"Searching: " + (double)(EndTime4 - EndTime3) / N * 60 + " seconds";
		rt.writeMsg(string_result);

		mt.stopMe();
		return string_result;
	}
	
}