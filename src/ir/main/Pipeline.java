package ir.main;

import java.util.Date;

import ir.cluster.VWDriver;
import ir.feature.FeatureExtraction;
import ir.index.Search;

public class Pipeline {

	// the main entry point for the Pipeline execution
	/** Setup
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
		//args[4]=0|1|2|3, the botlevel clustering method to choose, 0: serial; 1: MR job based, 2:  multi-thread, 3 multi-process
		// test arguments: data/images/ test/ 10 10 1
		 run(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]),Integer.parseInt(args[4]));
	}
	
	public static String run(String src, String dst, int topK, int botK, int botlvlcluster_type){
		long N = 1000 * 60;
		long startTime = new Date().getTime();
		
		//TODO: call the main entry point of the Feature Extraction
		System.out.println("\n\n\n\n\nFeature Extraction");
		String features = dst + "/data/features";// the feature folder
		FeatureExtraction.extractFeatures(src, dst + "/data/fn.txt", dst+"/data/features/", dst + "/temp/fe/");
		System.out.println("Features folder:"+features);
		
		long EndTime1 = new Date().getTime();
		
		//TODO: call the main entry point of the vocabulary construction and frequency generation
		System.out.println("\n\n\n\n\nvocabulary construction and frequency generation");
		String fs = dst + "/data/fs.seq";
		String[] args = {features, fs, dst, "" + topK, "" + botK};
		String s = VWDriver.run(args,botlvlcluster_type);
		
		long EndTime2 = new Date().getTime();
		
		//TODO: call the main entry point of the Indexing and Searching
		System.out.println("\n\n\n\n\nIndexing and Searching");
		//before run indexing, need to copy the frequency.txt file to local filesystem(index part reads from localfilesystem)---done 
		int clusterNum = topK * botK;
		Search.init(dst + "/data/frequency.txt", clusterNum, dst + "/cluster/clusters.txt");
		Search.runIndexing(dst + "/data/frequency.txt");
		long EndTime3 = new Date().getTime();
		//TODO: to test or evaluate here	
		Search.search(src + "/all_souls_000000.jpg");
		long EndTime4 = new Date().getTime();
		
		
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
		return string_result;
	}

}
