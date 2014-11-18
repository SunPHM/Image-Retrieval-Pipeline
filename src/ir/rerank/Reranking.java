package ir.rerank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.solr.client.solrj.SolrServerException;

import ir.index.GetMAP;
import ir.index.Search;
import ir.util.HadoopUtil;

public class Reranking {
	
	public static int numOfRerankedImages = 100;
	
	public static void main(String[] args){
		test();
	}
	
	public static void test(){
		String f = "/home/hadoop/Desktop/results/test14";
		Search.init(f + "/frequency_new.txt", 10000, f + "/clusters/", 100, 100);
		long num = Search.runIndexing(f + "/frequency_new.txt");
		double mAP = getRerankedMAP("/home/hadoop/Desktop/oxbuild_images", "data/gt", num);
		System.out.println("mAP = " + mAP);
	}
	
	public static double getRerankedMAP(String images,String gt, long total_images){
		// read all the files with "query"
		String[] files = HadoopUtil.getListOfFiles(gt);
		// store the image/mAP pairs
		HashMap<String,Double> images_AP = new HashMap<String,Double>();
		for(String file : files){			
			if(file.contains("query")){
				try {
					// get features and create query
					String[] features = GetMAP.getFeatures(file, images);
					String query = Search.createQuery_topdown(features);
					// run query
					String[] files_search_results = null;
					files_search_results = Search.query(query,(int)total_images);	
					//System.out.println("bucket size: " + total_images +"\t actual searched result size: " + files_search_results.length);
					rerank(query, files_search_results, images);
					// add the rerank here
					
					// double AP = GetMAP.calculateMAP(file, files_search_results);
					// images_AP.put(file, AP);
				} catch (SolrServerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		// get the average mAP of all queries' mAP
		double avg_mAP = 0;
		double num_images = 0;
		for (Map.Entry<String, Double> entry : images_AP.entrySet()) {
		    num_images ++;
		    avg_mAP += entry.getValue();
		}
		avg_mAP = avg_mAP / num_images;
		System.out.println("avg mAP is " + avg_mAP);
		return avg_mAP;
	}
	
	public static String[] rerank(String q, String[] searched_results, String images){
		String[] reranked_results = new String[searched_results.length];
		TreeMap<Double, String> tmap = new TreeMap<Double, String>(); // used for sorting
		double[] qh = stringToDoubleArray(q, Search.clusterNum);
		for(int i = 0; i < numOfRerankedImages; i++){
			String filename = searched_results[i].split("//")[1];
			double distance = getDistance(qh, images + "/" + filename);
			tmap.put(distance, filename);
		}
		tmap.values().toArray(reranked_results);
		for(int j = numOfRerankedImages; j < searched_results.length; j++) reranked_results[j] = searched_results[j];
		return reranked_results;
	}
	
	public static double getDistance (double[] qh, String file){
		double distance = 0;
		System.out.println(file);
		return distance;
	}
	
	public static double tfidfSimilarity(double[] a, double[] b){
		return 0;
	} // how to get the tf-idf vectors
	
	public static double[] stringToDoubleArray(String s, int length){
		double[] histogram = new double[length];
		String[] sa = s.split(" ");
		for(int i = 0; i < sa.length; i++){
			int x = Integer.parseInt(sa[i]);
			histogram[x] = histogram[x] + 1;
		}
		return histogram;
	}
	
	
	public static double computeSimilarity(double[] a, double[] b, int normType, int similarityType){
		// preprocess the double arrays according to normalization type
		double[] aa = new double[a.length]; 
		double[] bb = new double[b.length];
		if (normType == 0) { // raw vectors
			aa = a;
			bb = b;
		}
		if (normType == 1){ // l1 norm
			double anorm = l1Norm(a);
			double bnorm = l1Norm(b);
			for(int i = 0; i < a.length; i++){
				aa[i] = a[i] / anorm;
				bb[i] = b[i] / bnorm;
			}
		}
		if (normType == 2){ // l2 norm
			double anorm = l2Norm(a);
			double bnorm = l2Norm(b);
			for(int i = 0; i < a.length; i++){
				aa[i] = a[i] / anorm;
				bb[i] = b[i] / bnorm;
			}
		}
		
		// calculating similarity according to similarity type
		double distance = 0;
		if (similarityType == 0){ // cosine distance
			distance = cosineSimilarity(aa, bb);
		}
		if (similarityType == 1){ // l1 norm
			double[] cc = new double[aa.length];
			for (int j = 0; j < aa.length; j++) cc[j] = aa[j] - bb[j];
			distance = l1Norm(cc);
		}
		if (similarityType == 2){
			double[] cc = new double[aa.length];
			for (int j = 0; j < aa.length; j++) cc[j] = aa[j] - bb[j];
			distance = l2Norm(cc);
		}
		return distance;
	}
	
	public static double cosineSimilarity (double[] a, double[] b){
		double distance = 0;
		double ds = 0, an = 0, bn = 0;
		for(int j = 0; j < a.length; j++){
			ds += a[j] * b[j];
			an += a[j] * a[j];
			bn += b[j] * b[j];
		}
		distance = - ds / (Math.sqrt(an) * Math.sqrt(bn));
		return distance;
	}
	
	public static double l1Norm (double[] a){
		double distance = 0;
		for(int i = 0; i < a.length; i++){
			distance += Math.abs(a[i]);
		}
		return distance;
	}
	
	public static double l2Norm (double[] a){
		double distance = 0;
		for(int i = 0; i < a.length; i++){
			distance += Math.sqrt(Math.pow(a[i], 2));
		}
		return distance;
	}
}