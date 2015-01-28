package ir.main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import ir.index.GetMAP;
import ir.index.Search;
import ir.util.HadoopUtil;

public class getMAPfromClusteringResults {
	
	//args[0] : results output
	//args[1] : gt
	//args[2] : images 
	
	public static void main(String[] args) throws Exception{
		String[] allfolders = HadoopUtil.getListOfFolders(args[0]);
		String gt = args[1];
		String testImgFolder = args[2]; 
		
		for(String folder : allfolders){
			String foldername = new Path(folder).getName();
			int clusternumber = Integer.parseInt(foldername.split("_")[1]);
			System.out.println(clusternumber);
			
			//old frequency approach
			Search.init(folder + "/data/frequency.txt", clusternumber, folder + "/cluster/clusters.txt", clusternumber, 1);
			long total_images = Search.runIndexing(folder + "/data/frequency.txt");
			
			System.out.println("indexed images: " + total_images);
			
			double mAP = GetMAP.getmAP(testImgFolder, gt, total_images);
			
			System.out.println("MAP is " + mAP);
			
			BufferedWriter bw = new BufferedWriter(new FileWriter("mAP_result.txt",true));
			bw.write(folder + "\t" + mAP + "\n");
			bw.flush();bw.close();
			//break;
		}
	}

}
