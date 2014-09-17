package ir.util;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

public class XMLUtil {
	
	public static void main(String[] args) throws IOException, DocumentException{
		createConfiguration("output.xml", "frequency.txt", "clusters.txt", 100);
	}
	
	public static void createConfiguration(String path, String terms, String clusters, int clusterNum){
		
		Document document = DocumentHelper.createDocument();
	    Element root = document.addElement( "root" );
	    root.addAttribute("terms", terms);
	    root.addAttribute("clusters", clusters);
	    root.addAttribute("clusterNum", "" + clusterNum);
	    
		try {
			FileSystem fs = FileSystem.get(new Configuration());
		    XMLWriter writer = new XMLWriter(fs.create(new Path(path)));
		    writer.write(document);
		    writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    

	}
	
}