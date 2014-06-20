package ir.cluster;

import java.util.Date;

/**
 * The driver class for transformation, top-down clustering, and visual word frequency extraction
 * The input is features folder, topK, botK, result folder, output is a text file containing all the cluster centroids
 */
public class VWDriver {
	
	public static void main(String[] args){
		
	}
	
	public static String run(String[] args){
		// args[0]: the features folder
		// args[1]: the sequency file
		// args[2]: the result folder
		// args[3]: topK
		// args[4]: botK
		long N = 1000 * 60;
		long startTime = new Date().getTime();
		//TODO: use Tranform to transform text features into a sequency file
		Transform.run(args[0], args[1], args[2] + "/temp/seq");
		long EndTime1 = new Date().getTime();
		//TODO: call the top-down clustering
		String[] args1 = {args[1], args[2] + "/cluster/", args[3], args[4]};
		String t = TopDownClustering.run(args1);
		long EndTime2 = new Date().getTime();
		//TODO: call the frequency extractor
		int clusterNum = Integer.parseInt(args[3]) * Integer.parseInt(args[4]);
		Frequency.runJob(args[0], clusterNum, args[2] + "/cluster/clusters.txt", args[2] + "/temp/freq/", args[2] + "/data/frequency.txt");
		long EndTime3 = new Date().getTime();
		String s = "transformation time = " + (double)(EndTime1 - startTime)/N + "\n" +  
					"top-down clsutering time = " + (double)(EndTime2 - EndTime1)/N + "\n" +
					t + "frequency time = " + (double)(EndTime3 - EndTime2)/N + "\n";
		return s;
	}
	
}
