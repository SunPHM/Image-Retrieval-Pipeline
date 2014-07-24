package ir.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * Indexing runs locally using Solr
 */

public class Indexing {
	
	public static void main(String[] args) throws IOException, SolrServerException{
		//index("data/index/visual-word-frequency.txt");		
	}
	
	public static void index(String filename) throws IOException, SolrServerException{//indexing existing index matrix
		
		String urlString = "http://localhost:8983/solr";
		HttpSolrServer server = new HttpSolrServer(urlString);
		server.deleteByQuery( "*:*" );//clean the data in server
		
		//read index matrix from file
		
		//BufferedReader br = new BufferedReader(new FileReader(new File(filename)));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path infile=new Path(filename);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(infile)));
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		String line;
		while((line = br.readLine()) != null){
			SolrInputDocument doc = getDocument(line);
			docs.add(doc);
		}
		br.close();
		
		//System.out.println(docs.toArray()[0]);
		//System.out.println(docs.toArray()[1]);
		System.out.println("number of docs indexed:"+docs.size());
		
		server.add(docs);
	    server.commit();
	    System.out.println("indexing is done");
	}
	
	//for each line, construct an document
	public static SolrInputDocument getDocument(String line){
		SolrInputDocument doc = new SolrInputDocument();
		// add the id field
		String name = line.split("\t")[0];
		doc.addField("id", name);
		// add the cluster fields
		// index a numeric vector as a string
		String s = line.split("\t")[2];
		// includes field = term vector
		doc.addField("includes", s);
		//doc.set
		return doc;
	}
	
}

