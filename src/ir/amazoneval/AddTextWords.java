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
				"output/amazondata/run_1/topk_100_botk10/data/frequency_tw.txt",
				"output/amazondata/run_1/topk_100_botk10/data/freq.txt",
				"output/amazondata/run_1/topk_100_botk10/data/textwords.txt",
				new ArrayList<String>());
	}

	//add text words to frequency.txt
	//generate text words file in textwords.txt in the same folder as frequency.txt
	
	public static void addtw(String frequencytext, String input_dir, String output_freqtw,String output_freq, String output_tw, ArrayList<String> exclusionlist ) 
			throws IOException{
		String[] folders = HadoopUtil.getListOfFolders(input_dir);
		
		Path freqfile = new Path(frequencytext);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(freqfile)));
		BufferedWriter bw_freqtw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(output_freqtw),true)));
		BufferedWriter bw_freq = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(output_freq),true)));
		
		String inline = null;
		
		while((inline = br.readLine()) != null){
			String splits[] = inline.split("\\s+");
			//System.out.println(splits[0]);
			String parts[] = splits[0].split("/");
			String foldername = parts[parts.length - 2];
			
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
			String visualwords = "";
			for(int i = 2; i< splits.length; i ++){
				visualwords += " vw" + splits[i];
				textwords = textwords + " vw" + splits[i];
			}
			bw_freq.write(splits[0] + "\t" + splits[1] + "\t" + visualwords + "\n");
			bw_freqtw.write(splits[0] + "\t " + wordscount + "\t" + textwords + "\n");
		}
		bw_freqtw.flush();
		bw_freqtw.close();
		bw_freq.flush();
		bw_freq.close();
		br.close();
		
		
		
		//generate text words only -- also excluding images without vw to compare
		BufferedWriter bw_tw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(output_tw),true)));
		for(String str : folders){
			BufferedReader br_title=new BufferedReader(new InputStreamReader(fs.open(new Path(str + "/title.txt"))));
			String line = br_title.readLine();
			String[] parts = line.split("\\s+");
			String[] pathsplits = str.split("/");
			String id = pathsplits[pathsplits.length - 1];
			// rule out the images in the exclusion list
			if(exclusionlist.contains(id) == false){
				String record1 = id + "/1.JPG\t"  + parts.length + "\t" + line;
				String record2 = id + "/2.JPG\t"  + parts.length + "\t" + line;
				bw_tw.write(record1 + "\n");
				bw_tw.write(record2 + "\n");
			}
			br_title.close();
		}
		bw_tw.flush();
		bw_tw.close();
	}
	
}
