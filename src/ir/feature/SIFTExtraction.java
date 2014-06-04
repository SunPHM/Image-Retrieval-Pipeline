package ir.feature;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import net.semanticmetadata.lire.imageanalysis.sift.Extractor;
import net.semanticmetadata.lire.imageanalysis.sift.Feature;

public class SIFTExtraction {

	private static Extractor extractor = new Extractor();
	
	public static void main(String[] args) throws IOException {
		// test here
		// test HDFS
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("test.jpg");
		BufferedImage img = ImageIO.read(fs.open(path));
		getFeatures(img);
	}	
	
	public static String[] getFeatures(BufferedImage img) throws IOException {
	
		List<Feature> features = extractor.computeSiftFeatures(img);
		String[] results = new String[features.size()];
		for(int i = 0; i < features.size(); i++){
			String s = features.get(i).scale + " " + features.get(i).orientation + " " + features.get(i).location[0] + 
					" " + features.get(i).location[1] + " " + features.get(i).toString(); 
			//System.out.println(s);
			results[i] = s;
		}
		
		System.out.println("the number is " + features.size());
		return results;
		}
	
}
