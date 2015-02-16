package ir.amazoneval;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ir.util.HadoopUtil;

/*
 * Input: frequency.txt, root dir of the amazon data set of the 1030 objects. 	the key should be the picture
 * output: new frequency_tw.txt with a text words
 * */

public class AddTextWords {
	public static void main(String[] args) throws IOException{
		addtw("output/amazondata/run_1/topk_100_botk10/data/frequency.txt", "/media/windows_data/Academic/ImageRetrieval/Dihongs_dataset/1030-objects/",
				"output/amazondata/run_1/topk_100_botk10/data/frequency_tw.txt", new ArrayList<String>());
	}

	public static void addtw(String frequencytext, String input_dir, String output, ArrayList<String> exclusionlist ) 
			throws IOException{
		String[] folders = HadoopUtil.getListOfFolders(input_dir);
		
		Path freqfile=new Path(frequencytext);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(freqfile)));
		BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(output),true)));
		
		String inline = null;
		
		while((inline = br.readLine()) != null){
			String splits[] = inline.split("\\s+");
			String foldername = splits[0].split("/")[2];
			String textwords = null;
			//find where the text words are for this picture
			for(String f :  folders){
				if(f.endsWith(foldername) && exclusionlist.contains(foldername) == false){
					BufferedReader breader=new BufferedReader(new InputStreamReader(fs.open(new Path(f + "/title.txt"))));
					textwords = breader.readLine();
					breader.close();
					break;
				}
			}
			if(textwords == null){
				System.out.print("No text words for :" + foldername);
				continue;
			}
			int wordscount = Integer.parseInt(splits[1]) + textwords.split("\\s+").length;
			for(int i = 2; i< splits.length; i ++){
				textwords = textwords + " vw" + splits[i];
			}
			
			bw.write(splits[0] + "\t " + wordscount + " \t" + textwords + "\n");
		}
		bw.flush();bw.close();
		br.close();
		
	}
	
}
