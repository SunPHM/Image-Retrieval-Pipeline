package ir.cluster;

import java.io.IOException;
import java.util.Date;

//interface should be same to TopDownclustering
public class NormalKmeansClustering {

	public static String runKmeansClustering(String features, String cluster_output, int k){
		
		long ts0 = new Date().getTime();
		TopDownClustering.topLevelProcess(features, cluster_output+"top/cls", cluster_output+"/top/", k);
		// merge the clusters into a single file
		try {
			
			TopDownClustering.merge(cluster_output, cluster_output);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long ts3 = new Date().getTime();
		TopDownClustering.log("kmeans clustering ends with total process time: " + (double)(ts3 - ts0) / (60 * 1000) + " min");
		return "kmeans clustering ends with total process time: " + (double)(ts3 - ts0) / (60 * 1000) + " min\n";
	}
}
