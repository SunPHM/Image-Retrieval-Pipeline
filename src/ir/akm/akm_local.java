package ir.akm;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import ir.cluster.KMeans;
import ir.util.HadoopUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*local implemetation of Approximate k-means
 * input data format: sequencefile, with key/value = Text/VectorWritable
 * output cluster centroids file format: Text file, each line contains a cluster centroid
 * */ 
public class akm_local {

	int maxIterations = 10;
	int cluster_num = 100;
	double CovergenceDelta = 0.001;
	DistanceMeasure dm = new EuclideanDistanceMeasure();
	
	
	/*entry point of running akm
	 * @input_data_path: String, the input extractred features folder
	 * @ output_path: String, the output cluster centroids int txt file
	 * */
	public void run_akm(String input_data_path, String output_path) throws Exception{
		Configuration conf = new Configuration();
		// get the inital clusters
		Path initial_cluster_path = new Path(output_path + "/0");
		clusters_init_random(input_data_path, initial_cluster_path, cluster_num, conf , true);
		

		// run akm iteraterations until maximam iterations reached or cd reached
		int iteration_num = 0;
		while(iteration_num < maxIterations){
			Path clusters_in = new Path(output_path + "/" + iteration_num);
			Path clusters_out = new Path(output_path + "/" + (iteration_num + 1));
			//read the clusters into memory
			ArrayList<double[]> clusters = getClusterFromFile(conf, clusters_in);
			
			double[][] new_clusters = run_akm_one_iteration(input_data_path, clusters, conf);
			
			outputToFile(clusters_out, new_clusters, conf);
			
			iteration_num ++;
		}
		// convert the last iteration result to txt file "clusters.txt"
		outputFinalResult(output_path + "/" + iteration_num, output_path + "/clusters.txt", conf);
	}

	/*infile should be sequencefile of key/value = Text/VectorWritable
	 * outfile should be txt file : "clusters.txt"
	 * */
	private void outputFinalResult(String infile, String outfile, Configuration conf) throws IOException, InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		 FileSystem fs =FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(infile), conf);
		Text key =(Text) reader.getKeyClass().newInstance();
		VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
		
		
		FSDataOutputStream fsOutStream = fs.create(new Path(outfile));
		
		while(reader.next(key, value)){
	         StringBuilder sb=new StringBuilder();
	         sb.append(key.toString() + "    " + value.get().toString() + "\n");
	         byte[] byt=sb.toString().getBytes();
	         fsOutStream.write(byt);
		}
		reader.close();
		fsOutStream.flush();
		fsOutStream.close();
		
	}

	/*write the clusters out to sequencefile
	 * */
	private void outputToFile(Path clusters_out, double[][] new_clusters,	Configuration conf) throws IOException {
		// TODO Auto-generated method stub
		Text key = new Text();
		VectorWritable value = new VectorWritable();
		SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, clusters_out, Text.class,VectorWritable.class);
		for(int i = 0; i < new_clusters.length; i ++){
			key.set("" + i);
			value.set(new DenseVector(new_clusters[i]));
			writer.append(key, value);
		}
		writer.close();
	}

	/* one akm iteration
	 * @PARAM input_data_path: the whole data set path in string
	 * @PARAM clusters: the current clusters
	 * @PARAM conf
	 * @return the new clusters calculated 
	 * */
	private double[][] run_akm_one_iteration(String input_data_path, ArrayList<double[]> clusters, Configuration conf) 
			throws Exception {
		// TODO Auto-generated method stub
		// the number of elements assigned to each cluster
		int[] num_elements = new int[clusters.size()];
		for(int i =0; i< num_elements.length; i ++){
			num_elements[i] = 0;
		}
		
		double[][] new_clusters = new double[clusters.size()][128];
		
		//build random KDTree Forest
		KDTreeForest kdtf = new KDTreeForest();
		kdtf.build_forest(clusters);
		
		// iterate through the dataset (might contain multiple files)
		String[] allfiles = HadoopUtil.getListOfFiles(input_data_path);
		for(String in_file : allfiles){
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(in_file), conf);
			Text key =(Text) reader.getKeyClass().newInstance();
			VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
			while(reader.next(key, value)){
				Vector v = value.get();
				double[] q_vector = new double[128];
				for(int i = 0; i < 128; i ++){
					q_vector[i] = v.get(i);
				}
				int nnid = kdtf.nns_BBF(clusters, q_vector);
				//update the new_clusters and num_elements array
				for(int i = 0; i < 128; i ++){
					new_clusters[nnid][i] = (num_elements[nnid] * new_clusters[nnid][i] + q_vector[i])/(num_elements[nnid] + 1); 
				}
				num_elements[nnid] ++;
			}
		}
		// need to check empty clusters, i.e. that num_elements[i] = 0 clusters
		// set those clusters to the average of two random elements in the new_clusters
		//!!!!!! questionable approach, might lead to undesirable fluctuations
		for(int i = 0; i < num_elements.length; i ++){
			if(num_elements[i] == 0){
				Random rand = new Random();
				int one = rand.nextInt(new_clusters.length);
				while(one == i){
					one = rand.nextInt(new_clusters.length);
				}
				int two = rand.nextInt(new_clusters.length);
				while(two == i || two == one){
					two = rand.nextInt(new_clusters.length);
				}
				for(int j = 0; j < 128; j ++){
					new_clusters[i][j] = (new_clusters[one][j] + new_clusters[two][j]) / 2;
				}
			}
		}

		
		return new_clusters;
	}
	
	public void clusters_init_random(String input, Path clusters_path, int k, Configuration conf, boolean is_input_directory) 
			throws IOException, InstantiationException, IllegalAccessException{
		
		Path initial_path = null;
		if(is_input_directory == true){//is directory
			String[] input_all_files = HadoopUtil.getListOfFiles(input);
			System.out.println("\n!!!Generate random initial cls from path " + input_all_files[0] + "\n");
			initial_path = new Path(input_all_files[0]);
		}
		else initial_path = new Path(input);			
				
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), initial_path, conf);
		WritableComparable in_key = (WritableComparable)reader.getKeyClass().newInstance();
		VectorWritable in_value = (VectorWritable) reader.getValueClass().newInstance();
		//Get random K cluster, store in hashmap
		HashMap<Integer, VectorWritable> Random_K_cluster = new HashMap<Integer, VectorWritable>();
		int cluster_id = 0;
		Random rand = new Random();
		while(reader.next(in_key, in_value)){
			if(Random_K_cluster.size() < k){ //fill hashmap with k clusters
				Vector vec = in_value.get();
				VectorWritable cluster = new VectorWritable(vec);
				Random_K_cluster.put(cluster_id, cluster);
				cluster_id = cluster_id + 1;
			}
			else {//randomly replace some of the clusters.
				int flag = rand.nextInt(2);
				if(flag % 2 == 0){ //even, replace an existing random kluster.
					int index = rand.nextInt(k);// the cluster to replace 
					Vector vec = in_value.get();
					VectorWritable cluster = new VectorWritable(vec);
					Random_K_cluster.put(index, cluster);
				}
			}
		}
		
		reader.close(); 
		if(Random_K_cluster.size() != k)
			throw new IOException("kmeans init error, wrong number of initial clusters");
				
		SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, clusters_path, Text.class,VectorWritable.class);
		SortedSet<Integer> keys = new TreeSet<Integer>(Random_K_cluster.keySet());
		for (Integer out_key : keys) { 
			VectorWritable out_value = Random_K_cluster.get(out_key);
			writer.append(new Text("" + out_key), out_value);
		}
		writer.close();
	}

	/*read clusters into ArrayList from sequencefile
	 * */
	private ArrayList<double[]> getClusterFromFile(Configuration conf,	Path initial_cluster_path) 
			throws IOException, InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		ArrayList<double[]> clusters = new ArrayList();
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), initial_cluster_path, conf);
		Text key =(Text) reader.getKeyClass().newInstance();
		VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
		while(reader.next(key, value)){
			double[] new_cluster = new double[128];
			Vector v = value.get();
			for(int i = 0; i < 128; i ++){
				new_cluster[i] = v.get(i);
			}
			clusters.add(new_cluster);
		}
		return clusters;
	}
	///test use
	public static void main(String[] args) throws Exception{
		akm_local al = new akm_local();
		al.run_akm("test_fe_seq2seq_100images/data/features", "test_akm");
	}
	
}