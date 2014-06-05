package ir.cluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

public class TopDownClustering {
	
	private static String dm = "org.apache.mahout.common.distance.CosineDistanceMeasure";
	private static double delta = 0.001;
	private static int x = 100;
	
	private static int topK = 0;
	private static int botK = 0;
	
	public static void main(String[] args) throws IOException, InterruptedException{
		run(args);
	}
	
	public static void run(String[] args) throws IOException, InterruptedException{
		
		long ts0 = new Date().getTime();
		
		// parse parameters
		String data = args[0];
		String prefix = args[1];
		String top = prefix + "/top";
		String mid = prefix + "/mid";
		String bot = prefix + "/bot";
		String res = prefix + "/res";
		topK = Integer.parseInt(args[2]);
		botK = Integer.parseInt(args[3]);
		
		// execute pipeline
		clean(prefix);
		topLevelProcess(data, top + "/cls", top, topK);
		midLevelProcess(top, mid);
		// parallel bottom level clustering
		botLevelProcess(mid, bot, topK, botK, res);
		// merge the clusters into a single file
		merge(res, prefix);
		
		long ts1 = new Date().getTime();
		log("top-down clustering pipeline ends with total process time: " + (double)(ts1 - ts0) / (60 * 1000) + " min");
	}
	
	public static void clean(String prefix) throws IOException, InterruptedException{
		String cmd = "hadoop fs -rm -R " + prefix;
		run(cmd);
	}
	
	public static void topLevelProcess(String input, String cls, String top, int topK) throws IOException, InterruptedException{
		kmeans(input, cls, top, topK, delta, x);
	}
	
	public static void midLevelProcess(String top, String mid) throws IOException, InterruptedException{
		String command = "mahout clusterpp -i " + top + " -o " + mid + " -xm sequential";
		log(command);
		run(command);
	}
	
	public static void botLevelProcess(String mid, String bot, int topK, int botK, String res) throws IOException, InterruptedException{
		String[] folders = getFolders(mid);
		String cmd = "hadoop fs -mkdir " + res;
		run(cmd);
		
		Thread[] ts = new Thread[topK];
		
		for(int i = 0; i < folders.length; i++){
			
			//ts[i] = parrelBotProcess(folders[i] + "/part-m-0", 	bot + "/" + i + "/cls", 	bot + "/" + i, 	botK, 	delta, 	x, 
			//		bot + "/" + i + "/clusters-*-final/*", 	res + "/" + i, 	i);
			//change to serially processing
			
			//parrelBotProcess(String input, String clusters, String output, int k, double cd, int x, 
			//			String src, String dst, int num)
			String input=folders[i] + "/part-m-0";
			String clusters=bot + "/" + i + "/cls";
			String output=bot + "/" + i;
			int k=botK;
			double cd = delta;
			//int x=x;
			
			String src=bot + "/" + i + "/clusters-*-final/*";
			String dst=res + "/" + i;
			int num=i;
			
			String cmd0 = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
					+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl";
			//String cmd0 = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
			//		+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl -xm sequential";
			String cmd1 = "hadoop fs -mkdir " + dst;
			String cmd2 = "hadoop fs -cp " + src + " " + dst;
			//String cmd = cmd0 + "\n\r" + cmd1 + "\n\r" + cmd2;
			
			//Thread t = new Thread(new BotThread(cmd0, cmd1, cmd2, num));
			//t.start();
			
			log(cmd0);
			run(cmd0);
			log(cmd1);
			run(cmd1);
			log(cmd2);
			run(cmd2);
			log("thread" + num + "> ends");
			
		}
		
		for(int i = 0; i < ts.length; i++){
			ts[i].join();
		}
	}
	
	public static void merge(String src, String dst) throws IOException, InterruptedException{
		// copy and merge files
		// HadoopUtil.copyMerge(src, dst + "/tmp/tmp.txt");
		// mahout clusterdump
		String temp = "temptemptemp";
		String cmd0 = "mkdir " + temp;
		log(cmd0);
		run(cmd0);
		String cmd1 = "mahout clusterdump -i " + src + "/*/" + " -o " + temp + "/clusters.txt";
		log(cmd1);
		run(cmd1);
		String cmd2 = "hadoop fs -put " + temp + "/clusters.txt" + " " + dst;
		log(cmd2);
		run(cmd2);
		String cmd3 = "rm -r " + temp;
		log(cmd3);
		run(cmd3);
		//HadoopUtil.delete(dst + "/tmp/tmp.txt");
	}
	
	public static String[] getFolders(String mid) throws IOException, InterruptedException{
		
		String[] folders = new String[topK];
		int i = -1;
		String cmd = "hadoop fs -ls " + mid;
		log(cmd);
		
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec(cmd);
		String line;
		BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()) );
	    while ((line = in.readLine()) != null) {
	    	if(i == -1) i = 0;
	    	else{
	    		folders[i] = line.split(" +")[7];
	    		i++;
	    	}
	    }
	    in.close();
		p.waitFor();
		return folders;
	}
	
	public static void kmeans(String input, String clusters, String output, int k, double cd, int x) throws IOException, InterruptedException{
		String command = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
				+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl -xm sequential"  ;
		log(command);
		run(command);
	}
	
	public static Thread parrelBotProcess(String input, String clusters, String output, int k, double cd, int x, String src, String dst, int num) throws IOException, InterruptedException{
		
		String cmd0 = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
				+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl";
		//String cmd0 = "mahout kmeans -i " + input + " -c " + clusters + " -o " + output + " -k " + k + " "
		//		+ "-dm " + dm + " -cd " + cd + " -x " + x + " -ow -cl -xm sequential";
		String cmd1 = "hadoop fs -mkdir " + dst;
		String cmd2 = "hadoop fs -cp " + src + " " + dst;
		//String cmd = cmd0 + "\n\r" + cmd1 + "\n\r" + cmd2;
		
		Thread t = new Thread(new BotThread(cmd0, cmd1, cmd2, num));
		t.start();
		return t;
	}
	
	public static void copyResults(String cs, String res) throws IOException, InterruptedException{
		String cmd0 = "hadoop fs -mkdir " + res;
		run(cmd0);
		
		String cmd = "hadoop fs -cp " + cs + " " + res;
		log(cmd);
		run(cmd);
	}
	
	public static void log(String msg){
		Date now = new Date();
		System.out.println((now.getYear() - 100) + "/" + (now.getMonth() + 1) + "/" + now.getDate() + " " +  
				now.getHours() + ":" + now.getMinutes() + ":" + now.getSeconds() + ": " + msg);
	}
	
	public static void run(String command) throws IOException, InterruptedException{
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec(command);
	    p.waitFor();
	}
}
