package ir.index;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.solr.client.solrj.SolrServerException;

public class Evaluate {
	
	public static F1Score search(String[] features, String gt) throws IOException, SolrServerException{
		// get query from a set of images and measure the mean F1 score
		String query = Indexing.createQuery(features);
		// run query
		String[] files=Indexing.query(query);
		
		return Indexing.getF1Score(files, gt);
	}
	
	public static void evaluate(String folder) throws IOException, SolrServerException{
		//TODO: compare the word occurrence and TF-IDF
		// read all the files with "query"
		File fd = new File(folder);
		String[] files = fd.list();
		// store the F1 scores
		ArrayList<F1Score> list = new ArrayList<F1Score>();
		
		for(String file : files){
			if(file.contains("query")){
				BufferedReader reader = new BufferedReader(new FileReader(new File(folder + "/" + file)));
				String line = reader.readLine();
				reader.close();
				String[] array = line.split(" ");
				String queryImage = array[0].substring("oxc1_".length()) + ".jpg.txt" ;
				double lowX = Double.parseDouble(array[1]);
				double lowY = Double.parseDouble(array[2]);
				double highX = Double.parseDouble(array[3]);
				double highY = Double.parseDouble(array[4]);
				//System.out.println(line);
				//System.out.println(query + " " + lowX + " " + lowY + " " + highX + " " + highY);
				String[] features = getImageFeatures(Search.featureFolder + "/" + queryImage, lowX, lowY, highX, highY);
				// search the image features
				System.out.println("query image " + queryImage);
				F1Score fs = search(features, folder + "/" + file.substring(0, file.length() - "_query.txt".length()));
				list.add(fs);
			}
		}
		
		//print out the F1 Score information with the F1Score list
		double sumPrecision = 0;
		double sumRecall = 0;
		double sumF1 = 0;
		for(F1Score fs : list){
			sumPrecision += fs.precision;
			sumRecall += fs.recall;
			sumF1 += fs.F1;
		}
		
		System.out.println("sum precision = "+ sumPrecision);
		System.out.println("sum recall = " + sumRecall);
		System.out.println("sum F1 = " + sumF1);
		
		System.out.println("average precision = "+ sumPrecision / list.size());
		System.out.println("average recall = " + sumRecall / list.size());
		System.out.println("average F1 = " + sumF1 / list.size());
	}
	
	public static String[] getImageFeatures(String filename, double lowX, double lowY, double highX, double highY) throws IOException{
		// currently use local I/O, it is very easy to transfer to HDFS I/O
		BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));
		ArrayList<String> list = new ArrayList<String>();
		String line;
		while((line = reader.readLine()) != null){
			String cs = line.split("\\(")[1].split("\\)")[0];
			//System.out.println(line);
			double x = Double.parseDouble(cs.split(", ")[0]);
			double y = Double.parseDouble(cs.split(", ")[1]);
			//System.out.println(cs + " " + x + " " + y);
			if(lowX <= x && x <= highX && lowY <= y && y <= highY){
				list.add(line);
				//System.out.println(line);
			}
		}
		reader.close();
		return list.toArray(new String[list.size()]);
	}
	public static void main(String[] args) throws IOException, SolrServerException{
		evaluate("data/index/gt");
	}
}
