package ir.index;

import java.io.IOException;
import org.apache.solr.client.solrj.SolrServerException;
import ir.util.HadoopUtil;

public class EvaluateImageNet {
	
	public static void main(String[] args){
		Search.clusters = "test/cluster/clusters.txt";
		Search.clusterNum = 4;
		evaluate("data/test12/");
	}
	
	public static void evaluate(String testfolder){
		// get the list of image files
		String[] images = HadoopUtil.getListOfFiles(testfolder);
		//System.out.println("number of test images: " + images.length);
		double sp = 0.0; // sum precision of tests 
		// go through the images
		try {
			for(int i = 0; i < images.length; i++){
				String path = images[i];
				String[] features = Search.getImageFeatures(path);
				String synset = extractSynset(images[i]);
				//System.out.println("synset: " + synset);
				String qs = Search.createQuery(features);
				String[] results = Search.query(qs);
				//System.out.println("number of results: " + results.length);
				int cn = 0;
				for(int j = 0; j < results.length; j++){
					if(results[j].contains(synset)) 
						cn++;
				}
				//System.out.println(cn);
				double precision = (double)cn / results.length;
				sp += precision;	
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		double map = sp / images.length;
		System.out.println("mean average precison = " + map);
	}
	
	public static String extractSynset(String path){
		String[] arr = path.split("_")[0].split("/");
		return arr[arr.length - 1];
	}
	
}
