package ir.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;


//import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

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
		Path path=new Path(dirPath);
		Configuration conf = new Configuration();
		try {
			path.getFileSystem(conf).mkdirs(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to create directory : "+dirPath);
			e.printStackTrace();
		}
	}
	public static void cpdir(String srcPath, String dstPath){
		//minor fix on this line
		Path src=new Path(srcPath+"/");
		Path dst=new Path(dstPath);
		Configuration conf = new Configuration();
		try {
			FileUtil.copy(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst,false, true, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to cp directory : "+srcPath + " to "+dstPath);
			e.printStackTrace();
		}
	}
	public static void cpFile(String srcFile, String dstFile){
		Path src=new Path(srcFile);
		Path dst=new Path(dstFile);
		Configuration conf = new Configuration();
		try {
			FileUtil.copy(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst,false, true, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to cp File : "+srcFile + " to "+dstFile);
			e.printStackTrace();
		}
	}
	public static String[] getListOfFiles(String folder_path){
		File path=new File(folder_path);
		Configuration conf = new Configuration();
		File[] tmp_files=null;
		ArrayList<String> ListOfFiles=new ArrayList<String>();
		try {
			tmp_files=FileUtil.listFiles(path);
			for(File f:tmp_files){
				if(f.isDirectory()==false){
					ListOfFiles.add(f.getPath());
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] res=new String[ListOfFiles.size()];
		res=ListOfFiles.toArray(res);
		return res;
	}
	public static String[] getListOfFolders(String folder_path){
		File path=new File(folder_path);
		Configuration conf = new Configuration();
		File[] tmp_files=null;
		ArrayList<String> ListOfFiles=new ArrayList<String>();
		try {
			tmp_files=FileUtil.listFiles(path);
			for(File f:tmp_files){
				if(f.isDirectory()==true){
					ListOfFiles.add(f.getPath());
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] res=new String[ListOfFiles.size()];
		res=ListOfFiles.toArray(res);
		return res;
	}
	
}