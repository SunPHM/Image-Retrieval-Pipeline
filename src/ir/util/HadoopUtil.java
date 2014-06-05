package ir.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HadoopUtil {
	
	public static void copyMerge(String folder, String file){//copy files in an folder to form a big file
		Path src = new Path(folder);
		Path dst = new Path(file);
		Configuration conf = new Configuration();
		try {
			FileUtil.copyMerge(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst, false, conf, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void delete(String dirPath){
		Path path = new Path(dirPath);
		Configuration conf = new Configuration();
		try {
			FileUtil.fullyDelete(path.getFileSystem(conf), path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void mkdir(String dirPath){
		File p=new File(dirPath);
		
		try {
			FileUtils.forceMkdir(p);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to create directory : "+dirPath);
			e.printStackTrace();
		}
	}
}