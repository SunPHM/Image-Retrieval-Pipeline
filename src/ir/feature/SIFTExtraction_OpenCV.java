package ir.feature;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;
import org.opencv.highgui.Highgui;

public class SIFTExtraction_OpenCV {

	private static FeatureDetector fd = null;
	private static MatOfKeyPoint mkp = null;
	private static DescriptorExtractor de = null;
	public static boolean is_inited = false;
	
	//test main
	public static void main(String[] args){
//		init();
		double[][] features = extractSIFT("data/images/all_souls_000000.jpg");
		System.out.println(features[0].length + "\n" + features.length);
	}
	//init only once before feature extraction
	public static void init(){
		//intialize native lib etc
		if(is_inited == false){
		    System.loadLibrary(Core.NATIVE_LIBRARY_NAME );
		    fd = FeatureDetector.create(FeatureDetector.SIFT);
		    mkp = new MatOfKeyPoint();
		    de = DescriptorExtractor.create(DescriptorExtractor.SIFT);
		    is_inited = true;
		    System.out.println("OPENCV inited !!!!!!\n\n\n");
		}
	}
	public static double[][] extractSIFT(String image_path){
		init();
		Mat desc = new Mat();
		Mat img_mat =  Highgui.imread(image_path);//read in image to matrix
    	fd.detect(img_mat, mkp); // detect key points
	    de.compute(img_mat,mkp,desc );//extract sift features
	    
	    int num_rows = desc.rows();
	    int num_cols = desc.cols();
	    double features[][] = new double[num_rows][num_cols];
	    for(int i = 0; i < num_rows; i ++){
	    	for(int j = 0; j < num_cols; j ++){
	    		features[i][j] = desc.get(i, j)[0];
	    	}
	    }
	    return features;
	}


}
