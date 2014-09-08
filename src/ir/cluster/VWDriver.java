package ir.cluster;

import ir.util.XMLUtil;

import java.util.Date;

/**
 * The driver class for transformation, top-down clustering, and visual word frequency extraction
 * The input is features folder, topK, botK, result folder, output is a text file containing all the cluster centroids
 */
public class VWDriver {
	
	public static void main(String[] args){
		
	}
	
	public static String run(String[] args,int botlvlcluster_type){
		// args[0]: the features folder
		// args[1]: the result folder
		// args[2]: topK
		// args[3]: botK
		long N = 1000 * 60;
		long startTime = new Date().getTime();
		//call the top-down clustering
		String[] args1 = {args[0], args[1] + "/cluster/", args[2], args[3]};
		String t = TopDownClustering.run(args1, botlvlcluster_type);
		long EndTime1 = new Date().getTime();
		//call the frequency extractor
		int clusterNum = Integer.parseInt(args[2]) * Integer.parseInt(args[3]);
		Frequency.runJob(args[0], clusterNum, args[1] + "/cluster/clusters.txt", args[1] + "/temp/freq/", args[1] + "/data/frequency.txt");
		long EndTime2 = new Date().getTime();
		String s = 	"top-down clsutering time = " + (double)(EndTime1 - startTime)/N + "\n" +
					t + "frequency time = " + (double)(EndTime2 - EndTime1)/N + "\n";
		//create configuration xml
		XMLUtil.createConfiguration(args[1] + "/conf.xml", args[1] + "/data/frequency.txt", args[1] + "/cluster/clusters.txt", clusterNum);
		return s;
	}
	
}
