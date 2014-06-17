package ir.index;

import ir.cluster.Frequency;
import ir.feature.SIFTExtraction;

import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.solr.client.solrj.SolrServerException;

/**
 * Indexing and Searching runs locally using Solr
 *
 */

public class Search {
	
	public static int featureSize = 128;
	
	public static int clusterNum;
	public static String clusters;
	public static String terms;
	
	public static void main(String[] args) throws IOException, SolrServerException{
		// run indexing
		runIndexing("data/index/frequency.txt");
	}
	
	public static void init(String terms, int clusterNum, String clusters){
		Search.terms = terms;
		Search.clusterNum = clusterNum;
		Search.clusters = clusters;
	}
	
	//TODO: code cleaning and add an entry point function
	public static void runIndexing(String terms){
		try {
			Indexing.index(terms);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void search(String image){
		try {
			System.out.println("test image: " + image);
			String[] features = SIFTExtraction.getFeatures(ImageIO.read(new File(image)));
			String qs = Search.createQuery(features);
			String[] results = Indexing.query(qs);
			System.out.println("results length = " + results.length);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static String createQuery(String[] features) throws IOException{//transform an image into a Solr document or a field
		
		//String[] features = SIFTExtractor.extract(image);
		//System.out.println("query: " + image);
		double[][] clusters = Frequency.FEMap.readClusters(Search.clusters);
		int[] marks = new int[Search.clusterNum];
		
		for(int i = 0; i < features.length; i++){
			double[] feature = new double[Search.featureSize];
			String[] args = features[i].split(" ");
			for (int j = 0; j < Search.featureSize; j++)
				feature[j] = Double.parseDouble(args[j + 4]);
			int index = Frequency.FEMap.findBestCluster(feature, clusters);
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
		
}
