package ir.main;
import java.io.*;
import java.util.ArrayList;

public class runTestonOxbuilds {
	
	public static void main(String[] args) throws Exception{
		for(int i =0; i < 1; i++){
			args[1] = args[1] + "/run_" + i;
			run(args);
		}
	//	runTestonOxbuilds_kmeans.run(args);
	}
	public static void run(String args[])
			throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException{
		String input=args[0];
		String output=args[1];
		String gt=args[2];
		String testImgFolder=args[3];
		int clustering_type = 0;
		
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
			
			Pipeline_Oxbuilds_Evaluate.run(input, output+"/"+output_suffix, topbot.topk, topbot.botk, 2, gt, testImgFolder, clustering_type);
		}
		
	}

}

class topkbotk{
	public int topk;
	public int botk;
	public topkbotk(int i1,int i2){
		this.topk=i1;
		this.botk=i2;
	}
}
