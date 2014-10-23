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
public class getmAP {

	public static void main(String args[]) throws IOException{
		String pipeline_output = "oxbuild_100_100_oct22";
		int    topk = 100;
		int    botk = 100;
		String images = "/home/xiaofeng/workspace/oxe/oxbuild_images";
		String gt = "/home/xiaofeng/workspace/oxe/gt";
		
		String dst = pipeline_output;
		Search.init(dst + "/data/frequency_new.txt", topk * botk, dst + "/cluster/clusters", topk, botk);
		long num_docs = Search.runIndexing(dst + "/data/frequency_new.txt");
		
		String[] search_results = Search.search_topdown("all_souls_000000.jpg");
		for(String str : search_results){
			System.out.println(str);
		}
		
		
		getmAP(images,gt, num_docs);
	}
	

	public static double getmAP(String images,String gt, long total_images) throws IOException{
		
		
		// read all the files with "query"
		String[] files = HadoopUtil.getListOfFiles(gt);
		// store the image/mAP pairs
		HashMap<String,Double> images_AP = new HashMap<String,Double>();
		
		for(String file : files){			
			if(file.contains("query")){
				BufferedReader reader;
//				System.out.println("Calculating mAP for query file: " + file);
				FileSystem fs = FileSystem.get(new Configuration());
				reader = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
				String line = reader.readLine();
				reader.close();
				String[] array = line.split(" ");
				String queryImage = array[0].substring("oxc1_".length()) + ".jpg";
				System.out.println("Calculating AP for query image " + queryImage);
				double lowX = Double.parseDouble(array[1]);
				double lowY = Double.parseDouble(array[2]);
				double highX = Double.parseDouble(array[3]);
				double highY = Double.parseDouble(array[4]);
					//System.out.println(line);
					//System.out.println(queryImage + " " + lowX + " " + lowY + " " + highX + " " + highY);
				String[] features = EvaluateOxbuilds.getPartialImageFeatures(images + "/" + queryImage, lowX, lowY, highX, highY);
					// search the image features
					//System.out.println(file.substring(0, file.length() - "_query.txt".length()));
					
/*					//change the num_results and get the precision/recall curve
				HashMap<Double,Double> rec_pre = new HashMap<Double,Double>();
				for(int num_results = 1; num_results <= 5000; num_results = num_results + 30){
					F1Score f1s = EvaluateOxbuilds.searchFeatures(features, file.substring(0, file.length() - "_query.txt".length()),num_results);
					rec_pre.put(f1s.getRec(), f1s.getPre());
				}
*/
				String query = Search.createQuery_topdown(features);
				// run query
				String[] files_search_results = null;
				try {
					files_search_results = Search.query(query,(int)total_images);
				} catch (SolrServerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("bucket size: " + total_images +"\t actual searched result size: " + files_search_results.length);
				
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
					int ok_num = EvaluateOxbuilds.getMatches(cleaned_results, i, okSet);
					int good_num = EvaluateOxbuilds.getMatches(cleaned_results, i, goodSet);
					int relevant = getRelevance(cleaned_results[i],okSet,goodSet);
					if(relevant == 1 ) relevant_detected ++;
					double precision = ((double) ok_num + good_num) / (i + 1);
					AP += ( precision * relevant ) / num_relevant_docs;
				}
				System.out.println(relevant_detected + "\t" + num_relevant_docs);
				if(relevant_detected != num_relevant_docs){
					System.out.println("Error found that actual relevant docs and relevant detected  : "
							 + num_relevant_docs + "  " + relevant_detected + ", please check");
				}
					
				System.out.println("AP for image " + queryImage + " is " + AP);
				images_AP.put(queryImage, AP);
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
		for(int i =0;i<array.length;i++){
			String item = array[i].substring(0, array[i].length()-4);
			String splits[] = item.split("/");
			item = splits[splits.length-1];
			if(junk.contains(item)){
				//junk image, skip it
			}
			else {
				temp_results.add(array[i]);
				index ++;
			}
		}
		String result[] = new String[temp_results.size()];
		result = temp_results.toArray(result);
		return result;
	}
}
