package ir.index;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ir.util.HadoopUtil;

// for oxbuilding data set out put, get the mAP of the results
public class getmAP {

	public static void main(String args[]){
		String pipeline_output = args[0];
		int    clusterNum = Integer.parseInt(args[1]);
		String images = args[2];
		String gt = args[3];
		
		String dst = pipeline_output;
		Search.init(dst + "/data/frequency.txt", clusterNum, dst + "/cluster/clusters.txt");
		Search.runIndexing(dst + "/data/frequency.txt");
		getmAP(images,gt);
	}
	public static void getmAP(String images,String gt){
		
		
		// read all the files with "query"
		String[] files = HadoopUtil.getListOfFiles(gt);
		// store the image/mAP pairs
		HashMap<String,Double> images_mAP = new HashMap<String,Double>();
		
		for(String file : files){			
			if(file.contains("query")){
				BufferedReader reader;
				System.out.println("Calculating mAP for file: " + file);
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
					String[] features = EvaluateOxbuilds.getPartialImageFeatures(images + "/" + queryImage, lowX, lowY, highX, highY);
					// search the image features
					//System.out.println(file.substring(0, file.length() - "_query.txt".length()));
					
					//change the num_results and get the precision/recall curve
					HashMap<Double,Double> rec_pre = new HashMap<Double,Double>();
					for(int num_results = 1; num_results <= 21; num_results = num_results + 5){
						F1Score f1s = EvaluateOxbuilds.searchFeatures(features, file.substring(0, file.length() - "_query.txt".length()),num_results);
						rec_pre.put(f1s.getRec(), f1s.getPre());
					}
					
					//traverse the rec_pre by rec and print out them
					SortedSet<Double> recs = new TreeSet<Double>(rec_pre.keySet());
					System.out.println("\n\nrecall \t precision");
					for (Double rec : recs) { 
					   Double pre = rec_pre.get(rec);
					   System.out.println(rec + " \t " + pre);
					}
					
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
}
