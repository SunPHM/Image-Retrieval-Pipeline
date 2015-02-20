package ir.amazoneval;

import ir.cluster.Frequency;
import ir.cluster.NormalKmeansClustering;
import ir.feature.FeatureExtraction_seq;
import ir.feature.SIFTExtraction;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VectorWritable;

//amazon evaluation pipeline
//1. feature extraction
//2. clustering
//3. frequecy job
//4. get accuracy
public class amazoneval_pipeline {
	private static VectorWritable value = new VectorWritable();
	static ArrayList<String> train = new ArrayList<String>();
	static ArrayList<String> gallery = new ArrayList<String>();
	private static Text key = new Text();
	static final int  feature_length = 128;

	public static void main(String args[]) 
			throws IOException, InterruptedException, ReflectiveOperationException{
		String amazon_root = "/media/windows_data/Academic/ImageRetrieval/Dihongs_dataset";
		String output = "amazon_pipeline";
		run("output/amazondata/run_0/topk_50_botk10/data/features/part-r-00000", amazon_root, output, 10);
	}


	public static void run(String allfeaturesseq, String amazon_root, String output_root , int clusternum)
			throws IOException, InterruptedException, ReflectiveOperationException{
		int num = 1;
		 
		String output = output_root + "/" + num;
		featureExtraction(allfeaturesseq, amazon_root + "/10-fold-partition/" + num, amazon_root + "/1030-objects" , output);
		clustering(output, clusternum);
		Frequency.runJob(output + "/gallery/", clusternum, output + "/cluster/clusters.txt", output + "/temp/freq/", output + "/data/frequency.txt");
		
	}
	//@PARAM 1 : the train.txt to read in the files to evaluate
	public static void featureExtraction(String allfeaturesseq, String amazon_partition, String amazon_objects_root, String output_root) 
			throws IOException, IllegalAccessException, ReflectiveOperationException{
		//record the images to extract features from
		train.clear();
		BufferedReader br_train = new BufferedReader(new FileReader(new File(amazon_partition + "/train.txt")));
		String inline = null;
		while((inline = br_train.readLine()) != null){
			String parts[] = inline.split("\\s+");
			train.add(parts[0]);
			train.add(parts[1]);
		}
		br_train.close();
		
		//record the images to evaluatefeatures from
		gallery.clear();
		BufferedReader br_gallery = new BufferedReader(new FileReader(new File(amazon_partition + "/gallery.txt")));
		inline = null;
		while((inline = br_gallery.readLine()) != null){
			String parts[] = inline.split("\\s+");
			gallery.add(parts[0]);
		}
		br_gallery.close();
		
		write2seqfile(allfeaturesseq, amazon_objects_root, train, output_root + "/train/train.seq");
		write2seqfile(allfeaturesseq, amazon_objects_root, gallery, output_root + "/gallery/gallery.seq");
		
		
		
		
	}
	//get the features and write to seqfile
	private static void write2seqfile(String allfeaturesseq, String amazon_objects_root, ArrayList<String> images, String out_seqfile)
			throws IOException, ReflectiveOperationException, IllegalAccessException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter( fs, conf, new Path(out_seqfile), key.getClass(), value.getClass());
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(allfeaturesseq), conf);
		
		ArrayList<String> imagesToremove = new ArrayList<String>();
		while(reader.next(key, value)){
			String parts[] = key.toString().split("/");
			String imageid = parts[parts.length - 2] + "/" + parts[parts.length -1];
			if(images.contains(imageid)){
				writer.append(key, value);
				imagesToremove.add(imageid);
			}
		}
		reader.close();
		for(String imageid : imagesToremove){
			images.remove(imageid);
		}
		
/*		for(String image : images){
			key.set(image);
			//get the feature
			double [][] features = getFeaturesFromImage(amazon_objects_root + "/" + image);
			for(int i = 0; i < features.length; i++){
				value.set(new DenseVector(features[i]));
				writer.append(key, value);
			}
		}
*/
		writer.close();

		
	}
	private static double[][] getFeaturesFromImage(String string) throws IOException {
		// TODO Auto-generated method stub
		
		//use of lire
		BufferedImage img = ImageIO.read(new ByteArrayInputStream(FileUtils.readFileToByteArray(new File(string))));
		String[] features = SIFTExtraction.getFeatures(img);
		double all_features[][] = new double[features.length][feature_length];
		for(int i = 0; i < features.length; i++){
			///TODO change here to choose if use byte representation
			all_features[i] = FeatureExtraction_seq.FEMap.getPoints(features[i].split(" "), feature_length);
		}
		return all_features;
	}
	public static void clustering(String output_root, int clusternum) 
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		String t = NormalKmeansClustering.runKmeansClustering(output_root + "/train/", output_root + "/cluster/", clusternum);
	}
}
