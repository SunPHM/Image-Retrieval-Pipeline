package ir.main;

public class Pipeline {

	// the main entry point for the Pipeline execution
	/** Setup
	 * @Java: 1.6
	 * @Hadoop: 1.2.1
	 * @Mahout: 0.9
	 * @Solr: 4.6.1
	 */
	
	public static void main(String[] args) {
		//args[0]: the path to the images on HDFS or local file system
		//args[1]: the path of the output on HDFS or local file system
		run(args[0], args[1]);
	}
	
	public static void run(String src, String dst){
		
		//TODO: call the main entry point of the Feature Extraction
		
		//TODO: call the main entry point of the Clustering
		
		//TODO: call the main entry point of the Indexing and Searching
		
	}

}
