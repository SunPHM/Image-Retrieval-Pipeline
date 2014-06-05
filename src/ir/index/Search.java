package ir.index;

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
			String[] features = SIFTExtraction.getFeatures(ImageIO.read(new File(image)));
			String qs = Indexing.createQuery(features);
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
	
	public static void main(String[] args) throws IOException, SolrServerException{
		// transform files of features into files of cluster ids
		//getWordFrequence(fnames, "bw", termFile);
		// index the cluster ids files
		//InvertedIndexing.index(termFile);
		// evaluate the search system pipeline
		Evaluate.evaluate("gt");
	}
		
}
