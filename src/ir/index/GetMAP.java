package ir.index;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;

import ir.util.HadoopUtil;

// for oxbuilding data set out put, get the mAP of the results
public class GetMAP {

	public static void main(String args[]) throws IOException{
		String pipeline_output = "hkm_100_200";
		int    topk = 100;
		int    botk = 200;
		String images = "/home/xiaofeng/workspace/oxe/oxbuild_images";
		String gt = "/home/xiaofeng/workspace/oxe/gt";
		
		String dst = pipeline_output;
		Search.init(dst + "/data/frequency.txt", topk * botk, dst + "/cluster/clusters.txt", topk, botk);
		long num_docs = Search.runIndexing(dst + "/data/frequency.txt");
		
//		String[] search_results = Search.search_topdown("all_souls_000000.jpg");
///		for(String str : search_results){
//			System.out.println(str);
//		}
		
		getmAP(images,gt, num_docs);
	}
	

	public static double getmAP(String images,String gt, long total_images){
		// read all the files with "query"
		String[] files = HadoopUtil.getListOfFiles(gt);
		// store the image/mAP pairs
		HashMap<String,Double> images_AP = new HashMap<String,Double>();
	
		for(String file : files){			
			if(file.contains("query")){
				try {
					// get features and create query
					String[] features = getFeatures(file, images);
					
					//create the query -- choose one approach
					//topdown approach
					//String query = Search.createQuery_topdown(features);
					// normal frequency approach
					String query = Search.createQuery(features);
					
					// run query
					String[] files_search_results = null;
					files_search_results = Search.query(query,(int)total_images);	
					System.out.println("bucket size: " + total_images +"\t actual searched result size: " + files_search_results.length);
					double AP = calculateMAP(file, files_search_results);
					System.out.println("AP : " + AP);
					images_AP.put(file, AP);
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
		
		//TODO get the average mAP of all images' mAP
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
	
	public static double calculateMAP(String file, String[] files_search_results){
		// calculate mAP
		String query_image_prefix = file.substring(0, file.length() - "_query.txt".length());
		HashSet<String> goodSet = EvaluateOxbuilds.getFiles( query_image_prefix+ "_good.txt");
		HashSet<String> okSet = EvaluateOxbuilds.getFiles(query_image_prefix + "_ok.txt");
		HashSet<String> junkSet = EvaluateOxbuilds.getFiles(query_image_prefix + "_junk.txt");
		
		String[] cleaned_results = clean_array(files_search_results,junkSet);
		
		System.out.println("cleaned results size: " + cleaned_results.length + "\t junk size " + junkSet.size());
		
		double AP = 0;
		double num_relevant_docs = okSet.size() + goodSet.size();
		
		int relevant_detected = 0;
		for(int i = 0; i<cleaned_results.length; i++){
			int relevant = getRelevance(cleaned_results[i],okSet,goodSet);
			relevant_detected += relevant;
			double precision = ((double) relevant_detected) / (i + 1);
			AP += ( precision * relevant ) / num_relevant_docs;
		}
		System.out.println(relevant_detected + "\t" + num_relevant_docs);
		if(relevant_detected != num_relevant_docs){
			System.out.println("Error found that actual relevant docs and relevant detected  : "
					 + num_relevant_docs + "  " + relevant_detected + ", please check");
		}
		return AP;
	}
	
	public static String[] getFeatures(String file, String images) throws IOException{
		BufferedReader reader;
		//System.out.println("Calculating mAP for query file: " + file);
		FileSystem fs = FileSystem.get(new Configuration());
		reader = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
		String line = reader.readLine();
		reader.close();
		String[] array = line.split(" ");
		String queryImage = array[0].substring("oxc1_".length()) + ".jpg";
		System.out.println("query image " + queryImage);
		double lowX = Double.parseDouble(array[1]);
		double lowY = Double.parseDouble(array[2]);
		double highX = Double.parseDouble(array[3]);
		double highY = Double.parseDouble(array[4]);
		String[] features = EvaluateOxbuilds.getPartialImageFeatures(images + "/" + queryImage, lowX, lowY, highX, highY);
		return features;
	}
		
	private static int getRelevance(String string, HashSet<String> okSet,
			HashSet<String> goodSet) {
		// TODO Auto-generated method stub
		String[] array = string.split("/");
		String temp = array[array.length - 1];
		String item = (temp.split("\\.")[0]);
		
		if(okSet.contains(item) || goodSet.contains(item)){
			return 1;
		}
		return 0;
	}


	public static String[] clean_array(String[] array, HashSet<String> junk){
		ArrayList<String> temp_results = new ArrayList<String>();
		int index = 0;
		for(int i = 0;i < array.length; i++){
			//if (array[i].isEmpty()) System.out.println("empty");
			String item = array[i].substring(0, array[i].length() - 4);
			String splits[] = item.split("/");
			item = splits[splits.length-1];
			if( !junk.contains(item)){
				temp_results.add(array[i]);
				index ++;
			}
		}
		String result[] = new String[temp_results.size()];
		result = temp_results.toArray(result);
		return result;
	}
}
