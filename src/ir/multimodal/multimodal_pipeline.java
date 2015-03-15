package ir.multimodal;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;

import ir.cluster.Frequency;
import ir.cluster.VWDriver;
import ir.feature.FeatureExtraction_seq;
import ir.feature.SIFTExtraction;
import ir.feature.SIFTExtraction_OpenCV;
import ir.index.Search;
import ir.util.HadoopUtil;



public class multimodal_pipeline {
	static final boolean use_opencv = false;
	
	//store the relevant docs for each of the category
	static HashMap<String, Integer> num_per_category = new HashMap<String, Integer>();

	//step 1 FE
	//step 2 VW (clustering, frequency)
	//step 3 add TW to VW
	//step 4 index
	//step 5 search and compute mAP
	public static void main(String[] args) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, 
			IOException, InterruptedException, SolrServerException{
		
		//args[0]
		String eval_data_root = "multimodal"; //args[0]; //root folder of the eval data
		// eval_data_root -- mmd-1
		//                        folders of categories each containing the map.txt
		//                -- query 
		//args[1]
		String images_seqfile = "multimodal.seq"; //args[1]; //need to convert all the images in mmd-1 dir to a single seqfile first
		
		//args[2]
		String topk = "10"; //args[2];
		
		//args[3]
		String botk = "10"; //args[3];
		
		//args[4]
		String output = "multimodal_output";
		
		
		
		HadoopUtil.mkdir(output);
		
		//FE
		String features = output + "/data/features";
		FeatureExtraction_seq.extractFeatures(images_seqfile, features);
		
		String[] arguments = {features, output, topk, botk, eval_data_root};
		
		// run clustering and frequecy and calculate mAP (step 2 - 5)
		run(arguments);
		
		
	}

	
	
	//args[0] = input of extracted features
	//args[1] = output root folder
	//args [2] = topk
	//args[3] = botk
	//args[4] = eval_data_root
	// run clustering and frequecy and calculate mAP (step 2 - 5)
	public static void run(String[] args) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException,
			IOException, InterruptedException, SolrServerException{
		String features = args[0];
		String dst = args[1];
		String topk = args[2];
		String botk = args[3];
		String eval_data_root = args[4];
		//VW
		String[] arguments = {features, dst, "" + topk, "" + botk};
		String runningtime = VWDriver.run(arguments, 2,1);
		
		//need to provide those map.txt in each subfolders accordingly to get text words
		// will generate vw.txt, vw_tw.txt, tw.txt in dst/data dir
		String images_root = eval_data_root +"/mmd-1/";
		MultiFreq.run(images_root, dst, num_per_category);
		
		//indexing
		String indexfile = dst + "/data/vw_tw.txt";
		int clusterNum = Integer.parseInt(topk) * Integer.parseInt(botk);
		Search.init(indexfile, clusterNum, dst + "/cluster/clusters.txt", Integer.parseInt(topk), Integer.parseInt(botk));
		int indexedimages = (int) Search.runIndexing(indexfile);
		
		//evaluate
		String query_folder = eval_data_root + "/query";
		double mAP = evaluate(query_folder, indexedimages);
		
		System.out.println("mAP is : " + mAP);
		
	}

	//execute all the queries and return the mAP
	private static double evaluate(String query_folder, int indexed_images) 
			throws IOException, SolrServerException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path folder_path = new Path(query_folder + "/queries.txt");
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(folder_path)));
		
		String inline = null;
		HashMap<String, Double> all_aps = new HashMap<String, Double>();
		while((inline = br.readLine()) != null){
			String[] splits = inline.split("\\s+");
			String category = splits[0]; // category and | or search tw ???
			for(int i = 1; i < splits.length; i ++){
				String search_tw = category;
				String query_img = query_folder + "/" + splits[i];
				double ap = 0;
				try{
					ap = getAP(category, search_tw, query_img, indexed_images);
				}catch(java.lang.ArrayIndexOutOfBoundsException e){
					System.out.println("Faile to get AP for image " + query_img);
					e.printStackTrace();
					continue;
				}
				
				all_aps.put(query_img, ap);
			}
		}
		br.close();
		
		double mAP = 0;
		for(Double ap : all_aps.values()){
			mAP += ap;
		}
		mAP = mAP / all_aps.size();
		return mAP;
	}

	private static double getAP(String category, String search_tw, String query_image, int indexed_no) 
			throws IOException, SolrServerException{
		//create the query (with only vw)
		String qs_vw = createQuery(query_image);
		//get the qs of vw + tx
		String qs = qs_vw + " " + search_tw;
		System.out.println("QueryImage: " + query_image + ", query string: " + qs);
		
		//get the results from solr
		String[] results = Search.query(qs, indexed_no);
		
		
		double AP = 0;
		double num_relevant_docs = num_per_category.get(category);
		
		int relevant_detected = 0;
		for(int i = 0; i<results.length; i++){
			
			
			int relevant = getRelevance(results[i], category);
			relevant_detected += relevant;
			double precision = ((double) relevant_detected) / (i + 1);
			AP += ( precision * relevant ) / num_relevant_docs;
		}
		System.out.println(relevant_detected + "\t" + num_relevant_docs);
		if(relevant_detected != num_relevant_docs){
			System.out.println("Error found that actual relevant docs and relevant detected  : "
					 + num_relevant_docs + "  " + relevant_detected + " are not equal, please check(retrieve results lengh :" + results.length);
		}
		System.out.println("AP for Image : " + query_image + " is: " + AP);
		return AP;
	}
	
	private static int getRelevance(String filename, String category) {
		String[] splits = filename.split("/");
		if(splits[splits.length - 2].equals(category)){
			return 1;
		}
		else{
			return 0;
		}
	}

	static double[][] clusters = null;
	public static String createQuery(String img_path) throws IOException{//transform an image into a Solr document or a field
		
		if(clusters == null){
			clusters = Frequency.FreMap.readClusters(Search.clusters, Search.clusterNum);
		}
		
		int[] marks = new int[Search.clusterNum];
		double [][] all_features = null;
		
		//get features into double array all_features
		if(use_opencv == true){
			all_features = SIFTExtraction_OpenCV.extractSIFT(img_path);
		}
		//else use lire
		else{
			String features[] = Search.getImageFeatures(img_path);
			all_features = new double[features.length][Search.featureSize];
			for(int i = 0; i < all_features.length; i++){
				String[] args = features[i].split(" ");
				for (int j = 0; j < Search.featureSize; j++){
					all_features[i][j] = Double.parseDouble(args[j + 4]);
				}
			}						
		}
		
		
		for(int i = 0; i < all_features.length; i++){
//			int index = Frequency.FreMap.findBestCluster(feature, clusters);
			int index = Frequency.FreMap.findBestCluster(all_features[i], clusters);
			marks[index]++;
		}
			
		String result = "";
		for(int i = 0; i < Search.clusterNum; i++){
			for(int j = 0; j < marks[i]; j++){
				if(result.length() == 0) result += "vw" + i;
				else result += " vw" + i;
			}	
		}
		//System.out.println("query string: " + result);
		return result;
	}
}
