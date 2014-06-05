package ir.index;

import ir.cluster.Frequency;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

/**
 * @author Yang Peng
 * @library: different libraries need to be loaded from other programs
 */

public class Indexing {
	
	public static void main(String[] args) throws IOException, SolrServerException{
		//index("data/index/visual-word-frequency.txt");
		String[] files=query("all_souls_000013");
		getF1Score(files, "data/index/gt/all_souls_1");
		
	}
	
	
	public static void index(String filename) throws IOException, SolrServerException{//indexing existing index matrix
		
		String urlString = "http://localhost:8983/solr";
		HttpSolrServer server = new HttpSolrServer(urlString);
		server.deleteByQuery( "*:*" );//clean the data in server
		
		//read index matrix from file
		BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		String line;
		while((line = br.readLine()) != null){
			SolrInputDocument doc = getDocument(line);
			docs.add(doc);
		}
		br.close();
		
		System.out.println(docs.toArray()[0]);
		System.out.println(docs.toArray()[1]);
		System.out.println("number of docs indexed:"+docs.size());
		
		server.add(docs);
	    server.commit();
	    System.out.println("indexing is done");
	}
	
	//for each line, construct an document
	public static SolrInputDocument getDocument(String line){
		SolrInputDocument doc = new SolrInputDocument();
		// add the id field
		String name = line.split("\t")[0];
		doc.addField("id", name);
		// add the cluster fields
		// index a numeric vector as a string
		String s = line.split("\t")[2];
		// includes field = term vector
		doc.addField("includes", s);
		//doc.set
		return doc;
	}
	
	
	public static String createQuery(String[] features) throws IOException{//transform an image into a Solr document or a field
	
		//String[] features = SIFTExtractor.extract(image);
		//System.out.println("query: " + image);
		double[][] clusters = Frequency.FEMap.readClusters(Frequency.clusters);
		int[] marks = new int[Frequency.clusterNum];
		
		for(int i = 0; i < features.length; i++){
			double[] feature = new double[Search.featureSize];
			String[] args = features[i].split(" ");
			for (int j = 0; j < Search.featureSize; j++)
				feature[j] = Double.parseDouble(args[j + 10]);
			int index = Frequency.FEMap.findBestCluster(feature, clusters);
			marks[index]++;
		}
		
		String result = "";
		for(int i = 0; i < Frequency.clusterNum; i++){
			for(int j = 0; j < marks[i]; j++){
				if(result.length() == 0) result += i;
				else result += " " + i;
			}	
		}
		System.out.println("query string: " + result);
		return result;
	}
	
	
	public static String[] query(String s) throws SolrServerException{//query and output results
		
		//query a numeric vector as a string
		String urlString = "http://localhost:8983/solr";
		HttpSolrServer server = new HttpSolrServer(urlString);
		// search
	    SolrQuery query = new SolrQuery();
	    //query.setQuery("includes:" + s);
	    query.set("q", "includes:" + s);
	    
	    // get results		
	    QueryResponse qresponse = server.query(query);
	    SolrDocumentList list = qresponse.getResults();
	    String[] files = new String[list.size()];
	    for(int i = 0; i < list.size(); i++){
	    	System.out.println(list.get(i).getFieldValue("id"));
	    	files[i] = list.get(i).getFieldValue("id").toString();
	    }
	    
	    return files;
	}
	
	
	
	public static F1Score getF1Score(String[] files, String gt){
		
		HashSet<String> goodSet = getFiles(gt + "_good.txt");
		HashSet<String> okSet = getFiles(gt + "_ok.txt");
		//HashSet<String> junkSet = getFiles(gt + "_junk.txt");
		
		//int totalNum = goodSet.size() + okSet.size() + junkSet.size();
		int totalNum = goodSet.size() + okSet.size();
		int goodNum = getMatches(files, goodSet);
		int okNum = getMatches(files, okSet);
		//int junkNum = getMatches(files, junkSet);
		
		//double precision = (double)(goodNum + okNum + junkNum) / files.length;
		//double recall = (double)(goodNum + okNum + junkNum) / totalNum;
		double precision = (double)(goodNum + okNum) / files.length;
		double recall = (double)(goodNum + okNum) / totalNum;
		System.out.println("query precision is " + precision);
		System.out.println("query recall is " + recall);
		
		return new F1Score(precision, recall);
	}
	
	public static int getMatches(String[] files, HashSet<String> set){
		int num = 0;
		for(String s : files){
			if (set.contains(s)) num++;
		}
		return num;
	}
	
	public static HashSet<String> getFiles(String filename){
		HashSet<String> set = new HashSet<String>();
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(filename));
			String line;
			while((line = reader.readLine()) != null){
				set.add(line + ".jpg.txt");
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return set;
	}
	
}

class F1Score {
	double precision;
	double recall;
	double F1;
	F1Score(double pre, double re){
		precision = pre;
		recall = re;
		F1 = 2 * (precision * recall) / (precision + recall);
	}
}


