package ir.main;


import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;

import ir.cluster.VWDriver;
import ir.feature.FeatureExtraction;
import ir.feature.FeatureExtraction_seq;
import ir.index.EvaluateOxbuilds;
import ir.index.Search;
import ir.index.GetMAP;
import ir.util.HadoopUtil;
import ir.util.MeasureContainers;
import ir.util.RecordTime;

public class Pipeline_Oxbuilds_Evaluate {

	// the main entry point for the Pipeline execution
	/** Setup
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
		//args[4]=0|1|2, the botlevel clustering method to choose, 0: serial; 1: MR job based, 2:  multi-thread (valid only using topdown clustering)
		//args[5]: the ground truth folder
		//args[6]: the folder containing the images (not the seqfile input)
		//args[7]: clustering_type : 0 - Hierachical; 1 - Mahoutkmeans; 2 - AKM.
		// test arguments: data/images/ test/ 10 10 1
		int  runTopdownClustering = 0;
		if(args.length >= 8){
			if(args[7].equals("0")){
				System.out.println("\n\n\nUsing Kmeans clustering instead of Topdownclustering!!!!");
				runTopdownClustering = 0;
			}
		}
		run(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]), args[5],args[6],runTopdownClustering);
	}
	
	         //args[0]: the path to the images on HDFS or local file system
			//args[1]: the path of the output on HDFS or local file system
			//args[2]: the number of top-level clusters
			//args[3]: the number of bot-level clusters
			//args[4]=0|1|2, the botlevel clustering method to choose, 0: serial; 1: MR job based, 2:  multi-thread (valid only using topdown clustering)
			//args[5]: the ground truth folder
			//args[6]: the folder containing the images (not the seqfile input)
			//args[7]: clustering_type : 0 - Hierachical; 1 - Mahoutkmeans; 2 - AKM.
	public static String run(String src, String dst, int topK, int botK, int botlvlcluster_type, String gt, String testImgFolder,int clustering_type) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		long N = 1000 * 60;
		long startTime = new Date().getTime();
		HadoopUtil.delete(dst);
		Path srcPath = new Path(src);
		String rt_mc_filename_prefix = null;
		if(clustering_type == 0) {
			rt_mc_filename_prefix  = "OxbuildEvaluate_Hierachical"+ srcPath.getName()+"_topK_"+topK+"botK_"+botK+"_botlvlcluster_type_"+botlvlcluster_type;
			
		}
		else if(clustering_type == 1){
			rt_mc_filename_prefix  = "OxbuildEvaluate_Mkmeans"+ srcPath.getName()+"K_"+topK*botK+"_botlvlcluster_type_"+botlvlcluster_type;
		}
		else{
			rt_mc_filename_prefix  = "OxbuildEvaluate_AKM"+ srcPath.getName()+"K_"+topK*botK+"_botlvlcluster_type_"+botlvlcluster_type;
		}
		//record time of each phase to a file
		RecordTime rt = new RecordTime(rt_mc_filename_prefix+"record_timestamp.txt");
		rt.writeMsg("#Task: "+src+" "+dst+" "+topK+" "+botK+" "+botlvlcluster_type);
		//run the process to issue "hadoop job -list" and get the results in a file
		MeasureContainers mt = new  MeasureContainers(rt_mc_filename_prefix+"recordcontainers.txt");
		mt.start();
		
		//Feature Extraction
		rt.writeMsg("$FEStart$ "+new Date().getTime());
		System.out.println("\n\nFeature Extraction");
		String features = dst + "/data/features";// the feature folder
//		FeatureExtraction.extractFeatures(src, features, dst + "/temp/fe/");
		FeatureExtraction_seq.extractFeatures(src, features);
		System.out.println("Features folder:" + features);
		rt.writeMsg("$FEEnd$ "+new Date().getTime());
		long EndTime1 = new Date().getTime();
		
		//Vocabulary Construction and Frequency Generation
		rt.writeMsg("$VWStart$ "+new Date().getTime());
		System.out.println("\n\nvocabulary construction and frequency generation");
		String[] args = {features, dst, "" + topK, "" + botK};
		String s = VWDriver.run(args, botlvlcluster_type,clustering_type);
		rt.writeMsg("$VWEnd$ "+new Date().getTime());
		long EndTime2 = new Date().getTime();
		
		//Indexing and Searching
		rt.writeMsg("$ISStart$ "+new Date().getTime());
		System.out.println("\n\nIndexing and Searching");
		//before run indexing, need to copy the frequency.txt file to local filesystem(index part reads from localfilesystem)---done 
		int clusterNum = topK * botK;
		
		//old frequency approach
		Search.init(dst + "/data/frequency.txt", clusterNum, dst + "/cluster/clusters.txt", topK, botK);
		long total_images = Search.runIndexing(dst + "/data/frequency.txt");
		
		// topdown frequecy approach
		//Search.init(dst + "/data/frequency_new.txt", clusterNum, dst + "/cluster/clusters", topK, botK);
		//long total_images = Search.runIndexing(dst + "/data/frequency_new.txt");
		
		long EndTime3 = new Date().getTime();
		//to test or evaluate here
		//Search.search(src + "/all_souls_000000.jpg");
//		String evaluation_result=EvaluateOxbuilds.evaluate(testImgFolder, gt,20);
		long EndTime4 = new Date().getTime();
		rt.writeMsg("$ISEnd$ " + new Date().getTime());
		 
		//get the mAP
		double mAP = GetMAP.getmAP(testImgFolder, gt, total_images);
		
		
		String string_result="Total Running Time: "+ (double)(EndTime3 - startTime) / N 
				+"\nFeature Extraction: "+ (double)(EndTime1 - startTime) / N
			+"\nVVWDriver: "+ (double)(EndTime2 - EndTime1) / N + "\n" + s
				+"Indexing: "+ (double)(EndTime3 - EndTime2) / N * 60 + " seconds\n" +
				"Searching: " + (double)(EndTime4 - EndTime3) / N * 60 + " seconds"
				
				+"\n"//+ evaluation_result
				+ "\nThe mAP is " + mAP;
		System.out.println("\n\n*******************************************  Running Time in minutes ********************************************");
		System.out.println(string_result);

		
		rt.writeMsg(string_result);

		mt.stopMe();
		return string_result;
	}
	
}
