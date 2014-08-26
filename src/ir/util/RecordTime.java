package ir.util;

import java.io.FileWriter;
import java.io.IOException;

//record the start time of  and ending time of each phase
public class RecordTime {
	
	private String outfile=null;
	public RecordTime(String file){this.outfile=file;}
	public void writeMsg(String str){
		 FileWriter fw;
		try {
			fw = new FileWriter(this.outfile,true);
			fw.write(str+"\n");//appends the string to the file
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} //the true will append the new data
		   
	};
	

}
