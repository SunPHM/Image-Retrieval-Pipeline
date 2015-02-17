package ir.amazoneval;

import ir.cluster.Frequency;
import ir.index.Indexing;
import ir.index.Search;
import ir.util.HadoopUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

//evaluate on amazon data
//args[0] : the output root directory of IR pipeline (contains cluster, data ... dir)
//args[1] : the root directory of dihong's amazon dataset (contains "10-folder-partition" and "1030-objects")
//args[2] : the type of indexing and searching : 0 vw + tw; 1 vw only; 2 tw only

//note: use ir.img2seqfile.Img2seqfile_localfs.java to convert all the imagse in 1030-objects to a single seqfile before run IR pipeline

public class Amazon_evaluate {
	
	static final int num_search = 5;
	//static ArrayList<String> exclusionlist = new ArrayList<String>();
	
	public static void main(String[] args) throws IOException, Exception{
		//change the two input parameters here
		String IRoutput = "output/amazondata/run_0/topk_10_botk10/";//args[0]
		String amazon_root = "/media/windows_data/Academic/ImageRetrieval/Dihongs_dataset/";// args[1];
		int type = 1;
		
		
		getAccuracy(IRoutput, amazon_root, type);
		

	}
	//@PARAM type : 0 - vw + tw;  1 - vw only; 2 tw only
	public static String getAccuracy (String IRoutput, String amazon_root, int type) throws Exception{
		
		
		String evaluate = amazon_root + "/10-fold-partition";
		String amazondata = amazon_root + "/1030-objects/";
		
		//list of object ids to exclude from indexing and searching
		ArrayList<String> exclusionlist = getExclustionlist(amazondata, IRoutput + "/data/frequency.txt");
		
		String[] evaluate_folders = HadoopUtil.getListOfFolders(evaluate);
		
		//add text words and output to frequency_tw.txt, excluding the list
		AddTextWords.addtw(IRoutput + "/data/frequency.txt", amazondata, 
				IRoutput + "/data/frequency_tw.txt",
				IRoutput + "/data/freq.txt",
				IRoutput + "/data/textwords.txt", 
				exclusionlist);
		
		// run evaluation on each of the sub folder of "10-fold-partition"
		String all_results = "";
		int total_correct = 0;
		int total_query = 0;
		for(int i = 0; i < evaluate_folders.length; i ++){
			String result = evaluate(IRoutput, evaluate_folders[i], amazondata, exclusionlist, type);
			String results[] = result.split(":")[1].split("/"); 
			int num_correct  = Integer.parseInt(results[0].trim());
			int num_query    = Integer.parseInt(results[1].trim());
			total_correct    += num_correct;
			total_query      += num_query;
			all_results = all_results + evaluate_folders[i] + "\t" + result + "\n";
		}

		all_results = all_results + "\nAvg accuracy = " + (double)total_correct/total_query;
		System.out.println(all_results + "\nAvg accuracy = " + (double)total_correct/total_query);
		return all_results;
	}
	
	//exclusionlist contains obeject ids without both the 2 images vw words
	private static ArrayList<String> getExclustionlist(String amazondata,
			String frequency) throws IOException {
		// TODO Auto-generated method stub
		ArrayList<String> list = new ArrayList<String>();
		HashMap<String, Integer> hm = new HashMap<String, Integer>();
		String[] all_ids = HadoopUtil.getListOfFolders(amazondata);
		for(String id : all_ids){
			String[] splits = id.split("/");
			hm.put(splits[splits.length - 1], 0);
		}
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(frequency))));
		String inline = null;
		while ((inline = br.readLine()) != null){
			String parts[] = inline.split("/");
			String id = parts[2]; 
			if(hm.containsKey(id) == false){
				hm.put(id, 1);
			}
			else{
				int num = hm.get(id);
				hm.remove(id);
				hm.put(id, num+1);
			}
		}
		br.close();
		 Iterator<Entry<String, Integer>> it = hm.entrySet().iterator();
		 while(it.hasNext()){
			 Entry<String, Integer> pairs = it.next();
			 if(pairs.getValue() < 2){
				 list.add(pairs.getKey());
			 }
		 }
		return list;
	}

	//per run of evaluate
	public static String evaluate(String IRoutput, String evalfolder, String amazondata, ArrayList<String> exclusionlist, int type) 
			throws IOException, Exception{
			
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		//get the list of pictures to probe, exclude them from indexing
		BufferedReader br_probe=new BufferedReader(new InputStreamReader(fs.open(new Path(evalfolder + "/probe.txt"))));
		ArrayList<String> probelist = new ArrayList<String>();
		String inline = null;
		while ((inline = br_probe.readLine()) != null){
			probelist.add(inline);
		}
		br_probe.close();
		
		//get the list of gallery 
		ArrayList<String> gallery = new ArrayList<String>();
		BufferedReader br_gallery=new BufferedReader(new InputStreamReader(fs.open(new Path(evalfolder + "/gallery.txt"))));
		while ((inline = br_gallery.readLine()) != null){
			gallery.add(inline);
		}
		br_gallery.close();
		
		
		// Different index files to index for different types
		if(type == 0){
			//index the new frequency_tw.txt excluding pictures from the probelist
			index(IRoutput + "/data/frequency_tw.txt", probelist);
		}
		else if(type == 1){
			index(IRoutput + "/data/freq.txt", probelist);
		}
		else{
			index(IRoutput + "/data/textwords.txt", probelist);
		}
		
		//search the results
		Search.loadConfiguration(IRoutput + "/conf.xml");
		Search.terms = IRoutput + "/data/frequency_tw.txt";
		String result = search(probelist, gallery, Search.clusters, amazondata,  exclusionlist, type);
		return result;
		
	}
	
	//search the using the queries and compare with the gallery to determine if the search result is right, exclude pics in exclusion list from searching
	public static  String search(ArrayList<String> queries, ArrayList<String> gallery, String clusters,
			String amazondata, ArrayList<String> exclusionlist, int type)
			throws IOException, SolrServerException{
		ArrayList<Integer> results = new ArrayList<Integer>();
		
		
		int i = 0;
		int num_correct = 0;
		int num_excluded = 0;
		for(String query : queries){
			String splits[] = query.split("\\s+");
			
			//query if only the object ids are not in the exclusionlist
			if(exclusionlist.contains(splits[0].split("/")[0]) == false){
			
				//create query with VW
				String qs_vw = null;
				if(type == 0 || type == 1){
					String features[] = Search.getImageFeatures(amazondata + "/" + splits[0]);
					qs_vw = createQuery(features);
				}
				// create query with TW
				String qs_tw = "";
				for(int j = 1; j < splits.length; j ++ ){
					qs_tw = qs_tw + " " +  splits[j];
				}
				//composite query string depending on the type
				String qs = null;
				if(type == 0){
					qs = qs_vw + qs_tw;
				}
				else if(type == 1){
					qs = qs_vw;
				}
				else{
					qs = qs_tw;
				}
				System.out.println("For type : " + type + ", search picture: " + splits[0] + "; Search String: " + qs);
				
				String[] searchresults = Search.query(qs, num_search);
				String correct_result = gallery.get(i/3);
				
				boolean correct = false;
				
				//check if the search results contain the picture in the correct result
				for(String str : searchresults){
					if(str.endsWith(correct_result.split("\\s+")[0])){
						correct = true;
					}
				}
				
				if(correct == true){
					results.add(1);
					num_correct ++;
				}
				else{
					results.add(0);
				}
			}
			else {
				num_excluded ++;
				results.add(-1);
			}
			
			i ++;
			
		}
		String result_str = "Search result(correctnum/total queries  in terms of accuracy in top "+ num_search+"): " + num_correct + "/" + (queries.size() - num_excluded);
		System.out.println(result_str);
		
		return result_str;
		
	}
	public static String createQuery(String[] features) throws IOException{//transform an image into a Solr document or a field

		double[][] clusters = Frequency.FreMap.readClusters(Search.clusters, Search.clusterNum);
		int[] marks = new int[Search.clusterNum];
			
		for(int i = 0; i < features.length; i++){
			double[] feature = new double[Search.featureSize];
			String[] args = features[i].split(" ");
			for (int j = 0; j < Search.featureSize; j++)
				feature[j] = Double.parseDouble(args[j + 4]);
			int index = Frequency.FreMap.findBestCluster(feature, clusters);
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
	
	public static long index(String filename, List<String> probelist) throws IOException, SolrServerException{//indexing existing index matrix
		
		String urlString = Search.urlString;
		HttpSolrServer server = new HttpSolrServer(urlString);
		server.deleteByQuery( "*:*" );//clean the data in server
		long docs_total_size=0;
		//read index matrix from file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path infile=new Path(filename);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(infile)));
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		String line;
		int count_skipped = 0;
		while((line = br.readLine()) != null){
			boolean excluded = false;
			String[] querypic_parts = line.split("\\s+")[0].split("/");
			String querypic = querypic_parts[querypic_parts.length - 2] + "/" +querypic_parts[querypic_parts.length - 1];
			for(String str : probelist){
				if(querypic.equals(str.split("\\s+")[0]) == true){
					excluded = true;
					break;
				//	System.out.println(str + "\t\t" + line);
				}
			}
			if(excluded == true){
				System.out.println("skipped from probe.txt:" + line);
				count_skipped ++;
				continue;
			}
			
			SolrInputDocument doc = Indexing.getDocument(line);
			docs.add(doc);
			if(docs.size() >= Indexing.doc_buffer_size){
				server.add(docs);
			    server.commit();
			    docs_total_size = docs_total_size+docs.size();
			    System.out.println("indexed  " + (docs.size()) + " docs");
				docs.clear();
			}
		}
		br.close();
		System.out.println(docs.size());
		server.add(docs);
	    server.commit();
	    docs_total_size=docs_total_size+docs.size();
	    System.out.println("indexing is done, total docs indexed: "+docs_total_size + "\nSkipped total :" + count_skipped);
	    
	    return docs_total_size;
	}
	
}
