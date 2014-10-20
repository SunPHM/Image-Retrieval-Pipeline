package ir.main;

import ir.util.HadoopUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class runTestonImageNet {
	
	public static void main(String args[]) 
			throws NumberFormatException, IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException{
		String input = args[0];
		String output = args[1];
		String testImgFolder = args[2];
		boolean runTopdownClustering = true;
		
	/*	String[] files = HadoopUtil.getListOfFiles(input);
		for(String file:files){
			String output_suffix = "topk_"+50+"_botk"+50;
			Pipeline_ImageNet_Evaluation.run(file, output+"/"+output_suffix, 50, 50, 2, testImgFolder, runTopdownClustering);
		}
		
		for(String file:files){
			String output_suffix = "topk_"+100+"_botk"+100;
			Pipeline_ImageNet_Evaluation.run(file, output+"/"+output_suffix, 100, 100, 2, testImgFolder, runTopdownClustering);
		}
	*/
		BufferedReader br=new BufferedReader(new FileReader("topk_botK.txt"));
		ArrayList<topkbotk> list_topkbotk=new ArrayList<topkbotk>();
		String inline=null;
		while((inline=br.readLine())!=null){
			String splits[]=inline.split("\\s+");
			int i1 = Integer.parseInt(splits[0]);
			int i2 = Integer.parseInt(splits[1]);
			topkbotk newk = new topkbotk(i1,i2);
			list_topkbotk.add(newk);
			System.out.println("will run on "+newk.topk+"   "+newk.botk);
		};
		for(topkbotk topbot:list_topkbotk){
			String output_suffix="topk_"+topbot.topk+"_botk"+topbot.botk;
			
			Pipeline_ImageNet_Evaluation.run(input, output+"/"+output_suffix, topbot.topk, topbot.botk, 2, testImgFolder, runTopdownClustering);
		}
		
	}

}
