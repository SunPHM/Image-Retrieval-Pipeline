package ir.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

public class XMLUtil {
	
	public static void main(String[] args) throws IOException, DocumentException{
		createConfiguration("output.xml", "frequency.txt", "clusters.txt", 100);
	}
	
	public static void readDocument() throws DocumentException{
		SAXReader reader = new SAXReader();
        Document document = reader.read(new File("output.xml"));
        Node node = document.selectSingleNode( "//root/author" );
        String name = node.valueOf( "@name" );
        System.out.println(name);
	}
	
	public static void createConfiguration(String path, String terms, String clusters, int clusterNum) throws IOException{
		Document document = DocumentHelper.createDocument();
	    Element root = document.addElement( "root" );
	    root.addAttribute("terms", terms);
	    root.addAttribute("clusters", clusters);
	    root.addAttribute("clusterNum", "" + clusterNum);
	    
	    FileSystem fs = FileSystem.get(new Configuration());    
	    XMLWriter writer = new XMLWriter(fs.create(new Path(path)));
	    writer.write(document);
	    writer.close();
	}
	
}
