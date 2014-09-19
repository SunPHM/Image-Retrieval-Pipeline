package ir.cluster;

import java.io.IOException;

//interface should be same to TopDownclustering
public class NormalKmeansClustering {

	public static void runKmeansClustering(String features, String cluster_output, int k) throws IOException, InterruptedException{
		TopDownClustering.topLevelProcess(features, cluster_output+"top/cls", cluster_output+"/top", k);
		// merge the clusters into a single file
		TopDownClustering.merge(cluster_output, cluster_output);
	}
}
