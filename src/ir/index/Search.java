package ir.index;

import ir.cluster.Frequency;
import ir.cluster.TopDownFrequency;
import ir.feature.SIFTExtraction;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

/**
 * Indexing and Searching runs locally using Solr
 */

public class Search {
	
	public static int featureSize = 128;
	
	public static int clusterNum = 100;
	public static int topclusterNum = 10;
	public static int botclusterNum = 10;
	public static String clusters = "test/cluster/clusters.txt";
	public static String terms = "data/features/frequency.txt";
	
	public static String urlString = "http://localhost:8989/solr";
	public static int numOfResults = 20;
	
	public static void main(String[] args) throws IOException, SolrServerException{
		// run indexing
		//runIndexing("test/data/frequency.txt");
		//search("data/images/all_souls_000000.jpg");
		loadConfiguration("test/conf.xml");
		loadConfiguration_topdown("test/conf_new.xml");
		System.out.println(terms);
		System.out.println(clusters);
		System.out.println(clusterNum);
	}
	
	public static void init(String terms, int clusterNum, String clusters, int topclusterNum, int botclusterNum){
		Search.terms = terms;
		Search.clusterNum = clusterNum;
		Search.clusters = clusters;
		Search.topclusterNum = topclusterNum;
		Search.botclusterNum = botclusterNum;
	}
	
	public static void loadConfiguration(String path){
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			SAXReader reader = new SAXReader();
		    Document document = reader.read(fs.open(new Path(path)));
		    Element root = document.getRootElement();
		    Search.clusters = root.valueOf("@clusters");
		    Search.terms = root.valueOf("@terms");
		    Search.clusterNum = Integer.parseInt(root.valueOf("@clusterNum"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void loadConfiguration_topdown(String path){
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			SAXReader reader = new SAXReader();
		    Document document = reader.read(fs.open(new Path(path)));
		    Element root = document.getRootElement();
		    Search.clusters = root.valueOf("@clusters");
		    Search.terms = root.valueOf("@terms");
		    Search.topclusterNum = Integer.parseInt(root.valueOf("@topclusterNum"));
		    Search.botclusterNum = Integer.parseInt(root.valueOf("@botclusterNum"));
		    Search.clusterNum = Search.topclusterNum * Search.botclusterNum;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//TODO: code cleaning and add an entry point function
	public static long runIndexing(String terms){
		long docs_indexed=-1;
		try {
			docs_indexed = Indexing.index(terms);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return docs_indexed;
	}
	
	public static String[] search(String image){
		String[] results=null;
		try {
			System.out.println("test image: " + image);
			String[] features = getImageFeatures(image);
			String qs = Search.createQuery(features);
			results = query(qs);
			System.out.println("results length = " + results.length);
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;
		
	}
	public static String[] search_topdown(String image){
		String[] results=null;
		try {
			System.out.println("test image: " + image);
			String[] features = getImageFeatures(image);
			String qs = Search.createQuery_topdown(features);
			results = query(qs);
			System.out.println("results length = " + results.length);
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;
		
	}
	
	
	public static String[] getImageFeatures(String image){
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedImage img = ImageIO.read(fs.open(new Path(image)));
			String[] features = SIFTExtraction.getFeatures(img);
			return features;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
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
				if(result.length() == 0) result += i;
				else result += " " + i;
			}	
		}
		//System.out.println("query string: " + result);
		return result;
	}
	public static String createQuery_topdown(String[] features) throws IOException{//transform an image into a Solr document or a field

		double[][] clusters = Frequency.FreMap.readClusters(Search.clusters+"/0/0.txt", Search.topclusterNum);
//		int[] marks = new int[Search.clusterNum];
		
		String result = "";
		for(int i = 0; i < features.length; i++){
			double[] feature = new double[Search.featureSize];
			String[] args = features[i].split(" ");
			for (int j = 0; j < Search.featureSize; j++)
				feature[j] = Double.parseDouble(args[j + 4]);
			int index = TopDownFrequency.findGlobalClusterId(feature, Search.clusters, topclusterNum, botclusterNum);
			
			if(result.length() == 0){
				result = result + index;
			}
			else{
				result = result + " " + index;
			}
		}
			
/*		String result = "";
		for(int i = 0; i < Search.clusterNum; i++){
			for(int j = 0; j < marks[i]; j++){
				if(result.length() == 0) result += i;
				else result += " " + i;
			}	
		}
		//System.out.println("query string: " + result);	 
*/
		return result;
	}


	//query with customed number of results
	public static String[] query(String s, int num_results) throws SolrServerException{//query and output results
		
		//query a numeric vector as a string
		String urlString = Search.urlString;
		HttpSolrServer server = new HttpSolrServer(urlString);
		// search
	    SolrQuery query = new SolrQuery();
	    //query.setQuery("includes:" + s);
	    query.set("q", "includes:" + s);
	    query.setRows(num_results);
	    // get results		
	    QueryResponse qresponse = server.query(query);
	    SolrDocumentList list = qresponse.getResults();
	    String[] files = new String[list.size()];
	    for(int i = 0; i < list.size(); i++){
	    	//System.out.println(list.get(i).getFieldValue("id"));
	    	files[i] = list.get(i).getFieldValue("id").toString();
	    }
	    
	    return files;
	}
	
	//call with a default number of results
	public static String[] query(String s) throws SolrServerException{//query and output results
		
	    return query(s,Search.numOfResults);
	}
}
