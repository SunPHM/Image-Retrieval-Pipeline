package ir.main;
import java.io.*;
import java.util.ArrayList;

public class runTestonOxbuilds_kmeans {
	
	public static void main(String[] args) throws Exception{
		run(args);
	}
	public static void run(String[] args) throws Exception{
		String input=args[0];
		String output=args[1];
		String gt=args[2];
		String testImgFolder=args[3];
//		boolean runTopdownClustering=false;
		
		BufferedReader br=new BufferedReader(new FileReader("kmeans_k.txt"));
		ArrayList<Integer> list_topkbotk=new ArrayList<Integer>();
		String inline=null;
		while((inline=br.readLine())!=null){
			int i = Integer.parseInt(inline);
			list_topkbotk.add(new Integer(i));
			System.out.println("will run on "+i);
		};
		for(Integer k:list_topkbotk){
			String output_suffix="kmeans_K_"+k;
			Pipeline_Oxbuilds_Evaluate.run(input, output+"/"+output_suffix, k, 1, 1, gt, testImgFolder, 1);
		}
		
	}

}

