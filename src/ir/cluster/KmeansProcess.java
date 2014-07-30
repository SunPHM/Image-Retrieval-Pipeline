package ir.cluster;

import ir.util.HadoopUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


public class KmeansProcess {
	private static final CosineDistanceMeasure distance_measure = new CosineDistanceMeasure();
	private static int maxIterations = 100;
	
	public static void main(String[] args){
		kmeans(args[0],args[1],args[2],Integer.parseInt(args[3]),Double.parseDouble(args[4]),Integer.parseInt(args[5]));
	}
	public static void kmeans(String input, String clusters, String output, int k, double cd, int x) {
		
		org.apache.hadoop.conf.Configuration conf=new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			Path input_path = new Path(input);
			Path clusters_path = new Path(clusters+"/part-r-00000");
			Path output_path = new Path(output);
			HadoopUtil.delete(output);
			double clusterClassificationThreshold = 0;//////???	 
			 
			//read first K points from input folder as initial K clusters
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), input_path, conf);
			WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
			VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
			SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, clusters_path, Text.class,Kluster.class);
			for (int i = 0;i < k;i++){
				reader.next(key, value);
				 
				Vector vec = value.get();
				Kluster cluster = new Kluster(vec,i,distance_measure );
				writer.append(new Text(cluster.getIdentifier()), cluster);
			}
			reader.close();writer.close();
			System.out.println("initial "+k+"  clusters written to file: " + clusters);
			 
			KMeansDriver.run(conf, input_path, clusters_path, output_path, 
					distance_measure ,
					 cd,  maxIterations, true, clusterClassificationThreshold, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}

}
