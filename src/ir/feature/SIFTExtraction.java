package ir.feature;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.lucene.document.Field;

import net.semanticmetadata.lire.imageanalysis.sift.Extractor;
import net.semanticmetadata.lire.imageanalysis.sift.Feature;
import net.semanticmetadata.lire.impl.SiftDocumentBuilder;

public class SIFTExtraction {

	private static Extractor extractor = new Extractor();
	
	public static void main(String[] args) throws IOException {
		// test here
		BufferedImage img = ImageIO.read(new File("test.jpg"));
		getFeatures(img);
	}	
	
	public static String[] getFeatures(BufferedImage img) throws IOException {
	
		List<Feature> features = extractor.computeSiftFeatures(img);
		//String[] results = new String[features.size()];
		for(int i = 0; i < features.size(); i++){
			//features.get(i).scale; 
			//transform scale, orientation
			//String s = "";
			//s += features.toString();
			//results[i] = s;
			System.out.println(features.toString());
		}
		//SiftDocumentBuilder builder = new SiftDocumentBuilder();
			//Field[] fields = builder.createDescriptorFields(img);
			//String[] features= new String[fields.length];
			
			//for(int i =0 ; i < fields.length;i++){ 
			//		features[i] = fields[i].toString();
			//		//fields[i].;
			//		System.out.println(features[i]);
			//}
		
		// what is the difference between feature and field
		
		System.out.println("the number is " + features.size());
		return null;
			
			// about the results
	}

}
