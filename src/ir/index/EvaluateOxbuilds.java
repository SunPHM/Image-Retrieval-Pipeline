package ir.index;

import ir.feature.SIFTExtraction;
import ir.util.HadoopUtil;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;

public class EvaluateOxbuilds {
	
	public static void main(String[] args) throws IOException, SolrServerException{
		Search.clusters = "test/cluster/clusters.txt";
		Search.clusterNum = 4;
		evaluate("data/images", "data/gt_partial",20);
	}
	
	// benchmarking the Oxford 5k dataset
	public static String evaluate(String imageFolder, String gtFolder,int num_results){
		// read all the files with "query"
		String[] files = HadoopUtil.getListOfFiles(gtFolder);
		// store the F1 scores
		ArrayList<F1Score> list = new ArrayList<F1Score>();
		
		for(String file : files){			
			if(file.contains("query")){
				BufferedReader reader;
				//System.out.println(file);
				try {
					FileSystem fs = FileSystem.get(new Configuration());
					reader = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
					String line = reader.readLine();
					reader.close();
					String[] array = line.split(" ");
					String queryImage = array[0].substring("oxc1_".length()) + ".jpg";
					System.out.println("query image " + queryImage);
					double lowX = Double.parseDouble(array[1]);
					double lowY = Double.parseDouble(array[2]);
					double highX = Double.parseDouble(array[3]);
					double highY = Double.parseDouble(array[4]);
					//System.out.println(line);
					//System.out.println(queryImage + " " + lowX + " " + lowY + " " + highX + " " + highY);
					String[] features = getPartialImageFeatures(imageFolder + "/" + queryImage, lowX, lowY, highX, highY);
					// search the image features
					//System.out.println(file.substring(0, file.length() - "_query.txt".length()));
					F1Score f1s = searchFeatures(features, file.substring(0, file.length() - "_query.txt".length()),num_results);
					list.add(f1s);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
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
		return ( "average precision = "+ sumPrecision / list.size() + "\naverage recall = " + sumRecall / list.size()
				+"\naverage F1 = " + sumF1 / list.size());
	}

	// construct a query from one ground truth query, search them and get the F1 score
	public static F1Score searchFeatures(String[] features, String gt,int num_results) throws IOException{
		// get query from one image and measure F1 score
		String query = Search.createQuery(features);
		// run query
		String[] files = null;
		try {
			files = Search.query(query,num_results);
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// return F1 score
		return getF1Score(files, gt);
	}
	
	public static String[] getPartialImageFeatures(String filename, double lowX, double lowY, double highX, double highY) throws IOException{
		ArrayList<String> list = new ArrayList<String>();
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedImage img = ImageIO.read(fs.open(new Path(filename)));
		String[] allFeatures = SIFTExtraction.getFeatures(img);
		
		// filter features by coordinates
		for (int i = 0; i < allFeatures.length; i++){
			String line = allFeatures[i];
			//System.out.println(line);
			String lx = line.split(" ")[2];
			String ly = line.split(" ")[3];
			//System.out.println(line);
			double x = Double.parseDouble(lx);
			double y = Double.parseDouble(ly);
			//System.out.println(cs + " " + x + " " + y);
			if(lowX <= x && x <= highX && lowY <= y && y <= highY){
				list.add(line);
			}
		}
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

			FileSystem fs = FileSystem.get(new Configuration());
			reader = new BufferedReader(new InputStreamReader(fs.open(new Path(filename))));
//			reader = new BufferedReader(new FileReader(filename));
			String line;
			while((line = reader.readLine()) != null){
				set.add(line);
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
		HashSet<String> fset = new HashSet<String>();
		for(int i = 0; i < files.length; i++){
			String[] array = files[i].split("/");
			String temp = array[array.length - 1];
			fset.add(temp.split("\\.")[0]);
		}
		int num = 0;
		for(String s : set){
			//System.out.println("in getMatches: " + s);
			if (fset.contains(s)) num++;
		}
		//System.out.println("num = " + num);
		return num;
	}
}

