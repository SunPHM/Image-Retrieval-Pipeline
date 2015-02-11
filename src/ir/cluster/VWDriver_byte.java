package ir.cluster;

import ir.akm.AKM;
import ir.akmbyte.AKM_byte;
import ir.util.XMLUtil;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

/**
 * The driver class for transformation, top-down clustering, and visual word frequency extraction
 * The input is features folder, topK, botK, result folder, output is a text file containing all the cluster centroids
 */
public class VWDriver_byte {
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		// args[0] feature folder
		// args[1] result folder prefix
		// args[2] delta
		// args[3] distance measure type, 0 cosine, 1 euclidean
		// args[4] clusterInitType, 0 serial, 1 random
		double[] deltas = {0.001, 0.0005, 0.0001, 0.00005, 0.00001, 0.000005, 0.000001};
		for(int i = 0; i < deltas.length; i++){
			// run cluster serial
			KMeans.init(deltas[i], 0, 0);
			String[] fixedArgs = {args[0], args[1] + "0" + i, 100 + "", 100 + ""};
			run(fixedArgs, 2, 0);
			HashMap<String, String> map = new HashMap<String, String>();
			map.put("delta", "" + deltas[i]); map.put("dmType", "0"); map.put("clusterInitType", "0");
			XMLUtil.storeParameters(args[1] + "0" + i + "/parameters.xml", map);
			
			// run cluster random
			KMeans.init(deltas[i], 0, 1);
			String[] fixedArgs2 = {args[0], args[1] + "1" + i, 100 + "", 100 + ""};
			run(fixedArgs2, 2, 0);
			HashMap<String, String> map2 = new HashMap<String, String>();
			map2.put("delta", "" + deltas[i]); map2.put("dmType", "0"); map2.put("clusterInitType", "1");
			XMLUtil.storeParameters(args[1] + "1" + i + "/parameters.xml", map2);
		}
	}
	
	public static String run(String[] args, int botlvlcluster_type, int  clustering_type) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		// args[0]: the features folder
		// args[1]: the result folder
		// args[2]: topK
		// args[3]: botK
		long N = 1000 * 60;
		long startTime = new Date().getTime();
		//call the top-down clustering
		String[] args1 = {args[0], args[1] + "/cluster/", args[2], args[3]};
		
		String t = null;
		if(clustering_type == 0){//topdown clustering
				t = TopDownClustering.run(args1, botlvlcluster_type);
		}
		else if(clustering_type == 1){// //kmeans clustering
			int k = Integer.parseInt(args[2])*Integer.parseInt(args[3]);
				t = NormalKmeansClustering.runKmeansClustering(args[0], args[1]+"/cluster/", k);
		}
		else{
			//akm clustering
			int k = Integer.parseInt(args[2])*Integer.parseInt(args[3]);
			AKM_byte akm = new AKM_byte();
			akm.setParam(k, 0.001);
			try {
			//	al.run_akm(args[0], args[1] + "/cluster/");
				akm.runClustering(args[0], args[1] + "/cluster/");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		long EndTime1 = new Date().getTime();
		//call the frequency extractor
		int topclusterNum = Integer.parseInt(args[2]);
		int botclusterNum = Integer.parseInt(args[3]);
		int clusterNum = Integer.parseInt(args[2]) * Integer.parseInt(args[3]);
		
		//old frequency job 
		//Frequency.runJob(args[0], clusterNum, args[1] + "/cluster/clusters.txt", args[1] + "/temp/freq/", args[1] + "/data/frequency.txt");
		
		//old frequency job using float 
		Frequency_float.runJob(args[0], clusterNum, args[1] + "/cluster/clusters.txt", args[1] + "/temp/freq/", args[1] + "/data/frequency.txt");
		
		//create configuration xml
		XMLUtil.createConfiguration(args[1] + "/conf.xml", args[1] + "/data/frequency.txt", args[1] + "/cluster/clusters.txt", clusterNum);
		
		
		
		
		//topdown frequency
//		TopDownFrequency.runJob(args[0], topclusterNum, botclusterNum, args[1] + "/cluster/clusters/", args[1] + "/temp/tdfreq/", args[1] + "/data/frequency_new.txt");
//		XMLUtil.createConfiguration(args[1] + "/conf_new.xml", args[1] + "/data/frequency_new.txt", args[1] + "/cluster/clusters", 
//				topclusterNum, botclusterNum);
		
		
		long EndTime2 = new Date().getTime();
		String s = 	"clsutering time = " + (double)(EndTime1 - startTime)/N + "\n" +
					t + "frequency time = " + (double)(EndTime2 - EndTime1)/N + "\n";
		
		System.out.println(s);
		return s;
	}
	
}
