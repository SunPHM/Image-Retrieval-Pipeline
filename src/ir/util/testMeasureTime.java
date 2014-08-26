package ir.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class testMeasureTime {

	public static void main(String[] args) throws InterruptedException{
		List<String> list = new ArrayList<String>();  
		ProcessBuilder pb = null;     
		String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";  
//		String classpath = System.getProperty("java.class.path");  
		// list the files and directorys under C:\  
		list.add(java);  
//		list.add("-classpath");  
//		list.add(classpath);  
		list.add(MeasureContainerProcess.class.getName());  
		list.add("recordcontainers.txt");
		     
		pb = new ProcessBuilder(list);
		Process ps=null;
		try {
			ps = pb.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Thread.sleep(1000*5);
		ps.destroy();
		
	}
}
