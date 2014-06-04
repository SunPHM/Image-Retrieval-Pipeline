package ir.feature;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import net.semanticmetadata.lire.imageanalysis.sift.Extractor;
import net.semanticmetadata.lire.imageanalysis.sift.Feature;

public class SIFTExtraction {

	private static Extractor extractor = new Extractor();
	
	public static void main(String[] args) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedImage img = ImageIO.read(fs.open(new Path("test.jpg")));
		getFeatures(img);
		getNames("data/images", "data/temp/fn.txt");
	}	
	
	// read a folder of images from HDFS and store their names into a file on HDFS
	public static void getNames(String img_folder, String file){
        try{
            FileSystem fs = FileSystem.get(new Configuration()); // open the image folder
            FileStatus[] status = fs.listStatus(new Path(img_folder)); // get the list of images
            FSDataOutputStream out = fs.create(new Path(file)); // create output stream
            PrintWriter pw = new PrintWriter(out.getWrappedStream()); // create writer
            for (int i=0;i<status.length;i++){
            		//System.out.println(status[i].getPath().getName());
            		pw.println(status[i].getPath().getName());
            		//pw.flush();
            }
            pw.close();
            out.close();
            fs.close();
            System.out.println("image filenames extraction is done");
        }catch(Exception e){
            System.out.println("File not found");
        }
	}
	
	// read BufferedImage and return extracted features
	public static String[] getFeatures(BufferedImage img) throws IOException {
		List<Feature> features = extractor.computeSiftFeatures(img);
		String[] results = new String[features.size()];
		for(int i = 0; i < features.size(); i++){
			String s = features.get(i).scale + " " + features.get(i).orientation + " " + features.get(i).location[0] + 
					" " + features.get(i).location[1] + " " + features.get(i).toString(); 
			//System.out.println(s);
			results[i] = s;
		}
		//System.out.println("the number of features = " + features.size());
		return results;
	}
	
}
