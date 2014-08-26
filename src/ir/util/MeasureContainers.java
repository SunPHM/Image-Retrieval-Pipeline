package ir.util;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

// used to issue "hadoop job -list" command periodically and write the result to a file
public class MeasureContainers  extends Thread
{
	String output_log_file=null;
	//String[] commands = {"/bin/sh", "-c", "hadoop job -list"};
	String[] commands = {"/bin/sh", "-c", "ls -l"};
	long a_sec=1000;
	Runtime rt = Runtime.getRuntime();
	volatile boolean finished = false;

	  public void stopMe()
	  {
	    finished = true;
	  }
   public MeasureContainers(String out)
   {
	  this.output_log_file=out;
   }

   @Override
   public void run()
   {
	   this.finished=false;
	   while(!finished){
		   
			try {
				Process proc;
				String filename= this.output_log_file;
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
		
      
   }
}