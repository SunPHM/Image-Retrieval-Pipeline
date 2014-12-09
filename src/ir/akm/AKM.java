package ir.akm;

import ir.feature.SIFTExtraction;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

// map-reduce implementation of AKM using random KDTree forest
public class AKM {
	int maxIterations = 10;
	int cluster_num = 100;
	double CovergenceDelta = 0.001;
	DistanceMeasure dm = new EuclideanDistanceMeasure();
	
	/*entry point of AKM clustering
	 *@PARAM input_dataset: input folder of extracted features
	 *@PARAM output: the cluster result output folder
	 */
	public void runClustering(String input_dataset, String output) throws InstantiationException, IllegalAccessException, IOException{
		Configuration conf = new Configuration();
		// get the inital clusters
		Path initial_cluster_path = new Path(output + "/0/0");
		akm_local.clusters_init_random(input_dataset, initial_cluster_path, cluster_num, conf , true);
		
		// run iterations
		// run akm iteraterations until maximam iterations reached or cd reached
		int iteration_num = 0;
		while(iteration_num < maxIterations){
			System.out.println("AKM iteration : " + (iteration_num + 1));
			Path clusters_in = new Path(output + "/" + iteration_num);
			Path clusters_out = new Path(output + "/" + (iteration_num + 1));
			
			runIteration(input_dataset, clusters_in, clusters_out, conf);
			
			iteration_num ++;
		}
		
	}

	private void runIteration(String input_dataset, Path clusters_in, Path clusters_out, Configuration conf) {
		// TODO Auto-generated method stub
		
	}
	
	public static class AKM_Mapper extends  Mapper<Text, VectorWritable, IntWritable, VectorWritable> {
		public static KDTreeForest kdtf= null;
		public static int K = 0; //number of clusters 
		public static String clusters_in= null; // input folder of clusters
		public static double[][] varray = null; // clusters array
		
		@Override
		public void setup( Context context) {
			Configuration conf=context.getConfiguration();
			K = Integer.parseInt(conf.get("K"));
			clusters_in = conf.get("clusters_in");
			
			//TODO
			//read in the clusters and store then in the varry
			
			//TODO
			// construct kdtree forest
		}

		@Override
		public void map(Text key, VectorWritable value, Context context) throws IOException {	
			// for each feature in the dataset, assign to the nearest neighbor using kdtree forest
		}
		
	}
}
