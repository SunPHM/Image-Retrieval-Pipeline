package ir.main;

import ir.cluster.VWDriver;
import ir.feature.FeatureExtraction;
import ir.index.Search;

public class Pipeline {

	// the main entry point for the Pipeline execution
	/** Setup
	 * @Java: 1.6
	 * @Hadoop: 1.2.1
	 * @Mahout: 0.9
	 * @Solr: 4.6.1
	 */
	
	public static void main(String[] args) {
		//args[0]: the path to the images on HDFS or local file system
		//args[1]: the path of the output on HDFS or local file system
		//args[2]: the number of top-level clusters
		//args[3]: the number of bot-level clusters
		// test arguments: data/images/ test/ 10 10
		run(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
	}
	
	public static void run(String src, String dst, int topK, int botK){
		
		//TODO: call the main entry point of the Feature Extraction
		String features = dst + "/data/features";// the feature folder
		FeatureExtraction.extractFeatures(src, dst + "/data/fn.txt", dst+"/data/features/", dst + "/temp/fe/");
		System.out.println("Features folder:"+features);
		//TODO: call the main entry point of the vocabulary construction and frequency generation
		String fs = dst + "/data/fs.seq";
		//On HDFS features are in data/features instead of test/data/features ???????
		String[] args = {"data/features/", fs, dst, "" + topK, "" + botK};
		VWDriver.run(args);
		//TODO: call the main entry point of the Indexing and Searching
		//before run indexing, need to copy the frequency.txt file to local filesystem(index part reads from localfilesystem)---done
		int clusterNum = topK * botK;
		Search.runIndexing(dst + "/data/frequency.txt", clusterNum, dst + "/cluster/clusters.txt");
		//TODO: to test or evaluate here
	 
	}

}
