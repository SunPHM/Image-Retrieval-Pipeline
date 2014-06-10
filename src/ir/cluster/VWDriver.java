package ir.cluster;
/**
 * The driver class for transformation, top-down clustering, and visual word frequency extraction
 * The input is features folder, topK, botK, result folder, output is a text file containing all the cluster centroids
 */
public class VWDriver {
	
	public static void main(String[] args){
		
	}
	
	public static void run(String[] args){
		// args[0]: the features folder
		// args[1]: the sequency file
		// args[2]: the result folder
		// args[3]: topK
		// args[4]: botK
		
		//TODO: use Tranform to transform text features into a sequency file
		Transform.run(args[0], args[1], args[2] + "/temp/seq");
		//TODO: call the top-down clustering
		String[] args1 = {args[1], args[2] + "/cluster/", args[3], args[4]};
		TopDownClustering.run(args1);
		//TODO: call the frequency extractor
		int clusterNum = Integer.parseInt(args[3]) * Integer.parseInt(args[4]);
		Frequency.runJob(args[0], clusterNum, args[2] + "/cluster/clusters.txt", args[2] + "/temp/freq/", args[2] + "/data/frequency.txt");
	}
	
}
