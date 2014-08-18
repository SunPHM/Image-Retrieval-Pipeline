package ir.main;


import java.io.FileWriter;
import java.io.IOException;

import ir.util.HadoopUtil;
//example args: splitted_images output_july_25 10 10 results.txt (0|1)
//splitted_images: input_path where folders that contain different numbers of images exist.
//output: a folder of outputs
// 10 10 
//results.txt:
//0: serial botlevel clustering, 1: MR job botlevel processing
public class runAWBTest {
	public static void main(String[] args){
		System.out.println("Running tests on AWB");
		String result_file=args[4];
		String[] inputs=HadoopUtil.getListOfFolders(args[0]);
		int i=0;
		
/*		//serial botlevel processing
		for(String input:inputs){
			
			String[] arguments={input,args[1]+"/"+(i++)+"_0_serial", "10", "10","0"};
			System.out.println("Running  of " +input + "  " +args[1]+"/"+(i)+"_0"+" 10 10 0");
			String result=Pipeline.runPipeline(arguments);
			
			try
			{
			    String filename= result_file;
			    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			    fw.write(input+":\nbotlvlclusteringType:serial\n"+result+"\n\n\n\n\n");//appends the string to the file
			    fw.close();
			}
			catch(IOException ioe)
			{
			    System.err.println("IOException: " + ioe.getMessage());
			}
		}

		*/
		i=0;
		//MR Job botlevel processing
		for(String input:inputs){
			
			String[] arguments={input,args[1]+"/"+(i++)+"_1_MRJob", "10", "10","1"};
			System.out.println("Running  of " +input + "  " +args[1]+"/"+(i)+"_1"+" 10 10  1");
			String result=Pipeline.runPipeline(arguments);
			
			try
			{
			    String filename= result_file;
			    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			    fw.write(input+":\nbotlvlclusteringType:MR Job\n"+result+"\n\n\n\n\n");//appends the string to the file
			    fw.close();
			}
			catch(IOException ioe)
			{
			    System.err.println("IOException: " + ioe.getMessage());
			}
		}
		i=0;
		//Multi thread botlevel processing
		for(String input:inputs){
			
			String[] arguments={input,args[1]+"/"+(i++)+"_Multi_thread", "10", "10","2"};
			System.out.println("Running  of " +input + "  " +args[1]+"/"+(i++)+"_Multi_thread"+" 10 10  2");
			String result=Pipeline.runPipeline(arguments);
			
			try
			{
			    String filename= result_file;
			    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			    fw.write(input+":\nbotlvlclusteringType:multi_thread\n"+result+"\n\n\n\n\n");//appends the string to the file
			    fw.close();
			}
			catch(IOException ioe)
			{
			    System.err.println("IOException: " + ioe.getMessage());
			}
		}
		
		
		/***************different topk and botk**************/
		// args: input_path  output_path   result_file
/*		String result_file=args[2];
		i=0;
		//MR Job botlevel processing
		for(int K=1;K<=12;K=K+2){
			
			String[] arguments={args[0],args[1]+"/"+(i++)+"_1_MRJob", new Integer(K*2).toString(), new Integer(K*2).toString(),"1"};
			System.out.println("Running  of " +args[0] + "  " +args[1]+"/"+(i)+"_1"+" "+new Integer(K*2).toString()+"  1");
			String result=Pipeline.runPipeline(arguments);
			
			try
			{
			    String filename= result_file;
			    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			    fw.write(args[0]+" "+ new Integer(K*2).toString()+" "+new Integer(K*2).toString()+":\nbotlvlclusteringType:MR Job\n"+result+"\n\n\n\n\n");//appends the string to the file
			    fw.close();
			}
			catch(IOException ioe)
			{
			    System.err.println("IOException: " + ioe.getMessage());
			}
		}
		i=0;
		//Multithread Job botlevel processing
		for(int K=1;K<=12;K=K+2){
			
			String[] arguments={args[0],args[1]+"/"+(i++)+"_1_Multithread", new Integer(K*2).toString(), new Integer(K*2).toString(),"2"};
			System.out.println("Running  of " +args[0] + "  " +args[1]+"/"+(i)+"_1"+" "+new Integer(K*2).toString()+"  2");
			String result=Pipeline.runPipeline(arguments);
			
			try
			{
			    String filename= result_file;
			    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			    fw.write(args[0]+" "+ new Integer(K*2).toString()+" "+new Integer(K*2).toString()+":\nbotlvlclusteringType:multithread\n"+result+"\n\n\n\n\n");//appends the string to the file
			    fw.close();
			}
			catch(IOException ioe)
			{
			    System.err.println("IOException: " + ioe.getMessage());
			}
		}
		*/
	}
	

}
