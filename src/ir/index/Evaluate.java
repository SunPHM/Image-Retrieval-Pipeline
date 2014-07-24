package ir.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.solr.client.solrj.SolrServerException;

public class Evaluate {
	
	public static void main(String[] args) throws IOException, SolrServerException{
		evaluate("test/data/features", "data/gt");
	}
	
	// benchmarking the oxford 5k dataset
	public static void evaluate(String featureFolder, String gtFolder){
		// read all the files with "query"
		File fd = new File(gtFolder);
		String[] files = fd.list();
		// store the F1 scores
		ArrayList<F1Score> list = new ArrayList<F1Score>();
		
		for(String file : files){
			if(file.contains("query")){
				BufferedReader reader;
				try {
					reader = new BufferedReader(new FileReader(new File(gtFolder + "/" + file)));
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
					String[] features = getImageFeatures(featureFolder + "/" + queryImage, lowX, lowY, highX, highY);
					// search the image features
					System.out.println("query image " + queryImage);
					F1Score fs = searchFeatures(features, gtFolder + "/" + file.substring(0, file.length() - "_query.txt".length()));
					list.add(fs);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SolrServerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

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

	// construct a query from one ground truth query, search them and get the F1 score
	public static F1Score searchFeatures(String[] features, String gt) throws IOException, SolrServerException{
		// get query from one image and measure F1 score
		String query = Search.createQuery(features);
		// run query
		String[] files=	Search.query(query);
		// return F1 score
		return getF1Score(files, gt);
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
	
	public static int getMatches(String[] files, HashSet<String> set){
		int num = 0;
		for(String s : files){
			if (set.contains(s)) num++;
		}
		return num;
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
