package ir.util;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

public class MeasureContainerProcess {
	public static void main(String args[]){
		String[] commands = {"/bin/sh", "-c", "hadoop job -list"};
		long a_sec=1000;
		Runtime rt = Runtime.getRuntime();
		String output_log_file=args[0];
		   while(true){
			   
				try {
					Process proc;
					String filename= output_log_file;
				    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
				    
					proc = rt.exec(commands);
					BufferedReader stdInput = new BufferedReader(new 
				            InputStreamReader(proc.getInputStream()));
				       // read the output from the command
				    String s = null;
				    fw.write("TimeStamp: "+new Date().getTime()+"\n");
				    while ((s = stdInput.readLine()) != null) {
				       //System.out.println(s);
				       fw.write(s+"\n");//appends the string to the file
				      }
				   stdInput.close();
				    fw.close();
				    
				    Thread.sleep(3*a_sec);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		   }
		
	/*	List<String> list = new ArrayList<String>();  
						ProcessBuilder pb = null;  
						     
						String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";  
					String classpath = System.getProperty("java.class.path");  
						// list the files and directorys under C:\  
						list.add(java);  
						list.add("-classpath");  
						list.add(classpath);  
						list.add(KmeansProcess.class.getName());  
						list.add(input);  
						list.add(clusters);  
						list.add(output);
						list.add(""+k);
						list.add(""+cd);
						list.add(""+x);
						list.add(""+i);
						     
						pb = new ProcessBuilder(list);  
						ps = pb.start();
						
						*/
	}
}
