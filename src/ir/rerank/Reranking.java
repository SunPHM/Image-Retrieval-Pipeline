package ir.rerank;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;

import ir.index.GetMAP;
import ir.index.Search;
import ir.util.HadoopUtil;

public class Reranking {
	
	public static int numOfRerankedImages = 0;
	public static int normType = 0;
	public static int similarityType = 0;
	public static Map<String, String> map = null;
	public static int num = 5058;
	
	public static void main(String[] args){
		String f = "/home/yp/Desktop/test110";
		//Search.init(f + "/frequency_new.txt", 20000, f + "/clusters/", 100, 200);
		Search.init(f + "/frequency.txt", 20000, f + "/clusters.txt", 100, 200);
		//num = (int)Search.runIndexing(f + "/frequency_new.txt");
		num = (int)Search.runIndexing(f + "/frequency.txt");
		map = readMap(Search.terms);
		System.out.println("initialization is done");
		//System.out.println(map.keySet().iterator().next().toString());
		//System.out.println(map.values().iterator().next().toString());
		test();
	}
	
	public static void test(){
		double mAP = getRerankedMAP("/home/yp/Desktop/oxbuild_images", "data/gt", num);
		//System.out.println("mAP = " + mAP);
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
					//String query = Search.createQuery_topdown(features);
					String query = Search.createQuery(features);
					// run query
					String[] files_search_results = null;
					files_search_results = Search.query(query,(int)total_images);	
					//System.out.println("bucket size: " + total_images +"\t actual searched result size: " + files_search_results.length);
					// add the rerank here
					String[] reranked_results = rerank(query, files_search_results);
					double AP = GetMAP.calculateMAP(file, reranked_results);
					images_AP.put(file, AP);
					System.out.println(file + ": " + AP);
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
		double mAP = 0;
		double num_images = 0;
		for (Map.Entry<String, Double> entry : images_AP.entrySet()) {
		    num_images ++;
		    mAP += entry.getValue();
		}
		mAP = mAP / num_images;
		System.out.println("mAP is " + mAP);
		return mAP;
	}
	
	public static String[] rerank(String q, String[] searched_results){
		String[] reranked_results = new String[searched_results.length];
		HashMap<String, Double> hmap = new HashMap<String, Double>();
		double[] qh = stringToDoubleArray(q, Search.clusterNum);
		for(int i = 0; i < numOfRerankedImages; i++){
			String filename = searched_results[i].split("//")[1];
			double distance = getDistance(qh, filename);
			hmap.put(filename, distance);
		}
		ValueComparator vc = new ValueComparator(hmap);
		TreeMap<String, Double> tmap = new TreeMap<String, Double>(vc); // used for sorting
		tmap.putAll(hmap);
		//System.out.println("map sorting results");
		//System.out.println(tmap);
		String[] temp = new String[numOfRerankedImages];
		tmap.keySet().toArray(temp);
		//System.out.println(tmap.size());
		for(int x = 0; x < numOfRerankedImages; x++) reranked_results[x] = temp[x];
		for(int j = numOfRerankedImages; j < searched_results.length; j++) reranked_results[j] = searched_results[j];
		//System.out.println("rerank ends");
		return reranked_results;
	}
	
	public static Map<String, String> readMap(String file){
		HashMap<String, String> map = new HashMap<String, String>();
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FSDataInputStream input = fs.open(new Path(file));
			String line;
			while( (line = input.readLine()) != null){
				String key = line.split("\t")[0].split("//")[1];
				String value = line.split("\t")[2];
				map.put(key, value);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map;
	}
	
	
	public static double getDistance (double[] qh, String filename){
		double distance = 0;
		String s = map.get(filename);
		double[] ah = stringToDoubleArray(s, Search.clusterNum);
		distance = computeSimilarity(qh, ah, normType, similarityType);
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

class ValueComparator implements Comparator<String> {

    Map<String, Double> base;
    public ValueComparator(Map<String, Double> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return 1;
        } else {
            return -1;
        } // returning 0 would merge keys
    }
}