package ir.main;

import ir.util.HadoopUtil;

import java.io.IOException;

public class runTestonImageNet {
	
	public static void main(String args[]) throws NumberFormatException, IOException{
		String input = args[0];
		String output = args[1];
		String testImgFolder = args[2];
		boolean runTopdownClustering = true;
		
		String[] files = HadoopUtil.getListOfFiles(input);
		for(String file:files){
			String output_suffix = "topk_"+50+"_botk"+50;
			Pipeline_ImageNet_Evaluation.run(file, output+"/"+output_suffix, 50, 50, 2, testImgFolder, runTopdownClustering);
		}
		
		for(String file:files){
			String output_suffix = "topk_"+100+"_botk"+100;
			Pipeline_ImageNet_Evaluation.run(file, output+"/"+output_suffix, 100, 100, 2, testImgFolder, runTopdownClustering);
		}
		
		
	}

}
