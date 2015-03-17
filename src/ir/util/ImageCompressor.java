package ir.util;

import java.awt.AlphaComposite;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ImageCompressor {
	private static final int Max_WIDTH = 1024;
	private static final int Max_HEIGHT = 1024;
 
	
	public static void main(String [] args) throws IOException{
		String infolder ="multimodal";
		String outfolder = "multimodal_resized";
		
		FileSystem fs = FileSystem.get(new Configuration());

		compress(infolder, outfolder, fs);
	 
    }
	//recusive method
	// copy anything in in_folder to out_folder, converting any picture that is too large to lower pixel resolution
	// and keep the folder structure at the same time
	public static void compress(String in_folder, String out_folder, FileSystem fs)
			throws IOException{
		
		HadoopUtil.mkdir(out_folder);
		//cp the files first
		String[] files = HadoopUtil.getListOfFiles(in_folder);
		
		for(String file : files){
			Path infile = new Path(file);
			
			//non-pictures
			if(infile.getName().matches("([^\\s]+(\\.(?i)(jpg|JPG|JPEG|png|PNG|gif|GIF|bmp|BMP))$)") == false){
				HadoopUtil.cpFile(in_folder + "/" + infile.getName(), out_folder + "/" + infile.getName());
			}
			
			//pictures
			else{
				//System.out.println(file);
				try{
					BufferedImage img = ImageIO.read(fs.open(infile));
					if(img != null){ // if broken image, skip them
						if(img.getWidth() <= Max_WIDTH && img.getHeight() <= Max_HEIGHT){
							HadoopUtil.cpFile(in_folder + "/" + infile.getName(), out_folder + "/" + infile.getName());
						}
						else{
							int type = img.getType() == 0? BufferedImage.TYPE_INT_ARGB : img.getType();
							BufferedImage outImg = resizeImage(img, type);
							FSDataOutputStream outstream = fs.create(new Path(out_folder + "/" + infile.getName()));
							String[] splits = infile.getName().split("\\.");
							String suffix = splits[splits.length - 1];
							ImageIO.write(outImg, suffix.toLowerCase(), outstream);
							
						}
					}
				}catch(java.lang.ArrayIndexOutOfBoundsException e){
					System.out.println("cannot process file : " + file + ", please check!");
					e.printStackTrace();
				}catch(javax.imageio.IIOException e){
					System.out.println("cannot process file : " + file + ", please check!");
					e.printStackTrace();
				}

			}
		}
		//folders
		String[] folders = HadoopUtil.getListOfFolders(in_folder);
		for(String folder : folders){
			Path infolder = new Path(folder);
			compress(folder, out_folder + "/" + infolder.getName(), fs);
		}
		
	}
 
    private static BufferedImage resizeImage(BufferedImage originalImage, int type){
    	int original_width = originalImage.getWidth();
    	int original_height = originalImage.getHeight();
    	
    	double resize_ratio = 1;
    	if(original_width > Max_WIDTH){
    		resize_ratio = (double) Max_WIDTH / original_width;
    	}
    	if(original_height > Max_HEIGHT){
    		if(resize_ratio > (double) Max_HEIGHT / original_height){
    			resize_ratio = (double) Max_HEIGHT / original_height;
    		}
    	}
    	if(resize_ratio == 1){
    		return originalImage;
    	}
    	
    	int new_width = (int) (original_width * resize_ratio);
    	int new_height = (int) (original_height * resize_ratio);
		BufferedImage resizedImage = new BufferedImage(new_width, new_height, type);
		Graphics2D g = resizedImage.createGraphics();
		g.drawImage(originalImage, 0, 0, new_width, new_height, null);
		g.dispose();
	 
		return resizedImage;
    }
}
