package ir.index;

import java.io.IOException;
import org.apache.solr.client.solrj.SolrServerException;
import ir.util.HadoopUtil;

public class EvaluateImageNet {
	
	public static void main(String[] args){
		
	}
	
	public static void evaluate(String testfolder){
		// get the list of image files
		String[] images = HadoopUtil.getListOfFiles(testfolder);
		double sp = 0.0; // sum precision of tests 
		// go through the images
		try {
			for(int i = 0; i < images.length; i++){
				String path = testfolder + "/" + images[i];
				String[] features = Search.getImageFeatures(path);
				String synset = images[i].split("_")[0];
			
				String qs = Search.createQuery(features);
				String[] results = Search.query(qs);
				int cn = 0;
				for(int j = 0; j < results.length; j++){
					if(results[j].contains(synset)) 
						cn++;
				}		
				double precision = cn / results.length;
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
	
}
