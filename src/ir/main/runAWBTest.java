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
/*	public static void main(String[] args){
		
		String[] inputs=HadoopUtil.getListOfFolders(args[0]);
		int i=0;
		//serial botlevel processing
		for(String input:inputs){
			
			String[] arguments={input,args[1]+"/"+(i++)+"_0_serial", "10", "10","0"};
			System.out.println("Running  of " +input + "  " +args[1]+"/"+(i)+"_0"+" 10 10 ");
			String result=Pipeline.runPipeline(arguments);
			
			try
			{
			    String filename= args[4];
			    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			    fw.write(input+":\nbotlvlclusteringType:serial\n"+result+"\n\n\n\n\n");//appends the string to the file
			    fw.close();
			}
			catch(IOException ioe)
			{
			    System.err.println("IOException: " + ioe.getMessage());
			}
		}
		i=0;
		//MR Job botlevel processing
		for(String input:inputs){
			
			String[] arguments={input,args[1]+"/"+(i++)+"_1_MRJob", "10", "10","1"};
			System.out.println("Running  of " +input + "  " +args[1]+"/"+(i)+"_1"+" 10 10 ");
			String result=Pipeline.runPipeline(arguments);
			
			try
			{
			    String filename= args[4];
			    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			    fw.write(input+":\nbotlvlclusteringType:MR Job\n"+result+"\n\n\n\n\n");//appends the string to the file
			    fw.close();
			}
			catch(IOException ioe)
			{
			    System.err.println("IOException: " + ioe.getMessage());
			}
		}
	}
	*/

}
