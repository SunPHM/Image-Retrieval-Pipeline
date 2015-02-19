package ir.amazoneval;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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
				"output/amazondata/run_1/topk_100_botk10/data/freqoid.txt",
				"output/amazondata/run_1/topk_100_botk10/data/freqoid_tw.txt",
				new ArrayList<String>());
	}

	//add text words to frequency.txt
	//generate text words file in textwords.txt in the same folder as frequency.txt
	
	public static void addtw(String frequencytext, String input_dir, 
			String output_freqtw,
			String output_freq, 
			String output_tw, 
			String output_freq_oid,
			String output_freq_oid_tw,
			
			ArrayList<String> exclusionlist ) 
			throws IOException{
		String[] folders = HadoopUtil.getListOfFolders(input_dir);
		
		Path freqfile = new Path(frequencytext);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(freqfile)));
		BufferedWriter bw_freqtw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(output_freqtw),true)));
		BufferedWriter bw_freq = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(output_freq),true)));
		
		HashMap<String, String> freq_OID = new HashMap<String, String>();
		HashMap<String, String> freq_OID_TW = new HashMap<String,String>();
		
		String inline = null;
		
		while((inline = br.readLine()) != null){
			String splits[] = inline.split("\\s+");
			//System.out.println(splits[0]);
			String parts[] = splits[0].split("/");
			String OID = parts[parts.length - 2];
			
			String textwords = null;
			//find where the text words are for this picture
			for(String f :  folders){
				if(f.endsWith(OID) && exclusionlist.contains(OID) == false){
					BufferedReader breader=new BufferedReader(new InputStreamReader(fs.open(new Path(f + "/title.txt"))));
					textwords = breader.readLine();
					breader.close();
					break;
				}
			}
			if(textwords == null){
				System.out.print("No text words for :" + OID);
				continue;
			}
			
			
			//put the visual words for further use
			if(freq_OID.containsKey(OID) == false){
				freq_OID.put(OID, inline);
			}
			else {
				String old = freq_OID.get(OID);
				String[] oldparts = old.split("\\s");
				String oldwords = " ";
				for(int i = 2; i < oldparts.length; i ++){
					oldwords += " " + oldparts[i];
				}
				freq_OID.remove(OID);
				freq_OID.put(OID, inline + oldwords);
			}
			//put the text words for further use
			if(freq_OID_TW.containsKey(OID) == false){
				freq_OID_TW.put(OID, textwords);
			}
			
			
			int wordscount = Integer.parseInt(splits[1]) + textwords.split("\\s+").length;
			String visualwords = "";
			for(int i = 2; i< splits.length; i ++){
				visualwords += " vw" + splits[i];
				textwords = textwords + " vw" + splits[i];//append visual words to textwords
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
		
		//get the freq file of objects, use the average VW:
		BufferedWriter bw_freq_OID = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(output_freq_oid),true)));
		BufferedWriter bw_freq_OID_TW = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(output_freq_oid_tw),true)));
		Iterator< Entry<String, String>> it = freq_OID.entrySet().iterator();
		HashMap<String, Integer> combinedVW = new HashMap<String, Integer>();
		while(it.hasNext()){
			Entry<String, String> pair = it.next();
			String OID = pair.getKey();
			//process the vw to get the histogram
//			System.out.println(pair.getValue());
			combinedVW.clear();
			String parts[] = pair.getValue().split("\\s+");
			for(int i = 2; i < parts.length; i ++){
				if(combinedVW.containsKey(parts[i].trim()) == false){
					combinedVW.put(parts[i], 1);
				}
				else{
					int num = combinedVW.get(parts[i]);
					combinedVW.remove(parts[i]);
					combinedVW.put(parts[i], num+1);
				}
			}
			//write the vws to output
			int  vw_num = 0;
		    String visualwords = "";
			Iterator< Entry<String, Integer>> iter = combinedVW.entrySet().iterator();
			while(iter.hasNext()){
				Entry<String, Integer> vwpair = iter.next();
				int num_repeat = (int) Math.ceil( ((double)vwpair.getValue()) / 2 );
				for(int i = 0; i < num_repeat; i ++){
					visualwords += " vw" + vwpair.getKey();
				}
				vw_num += num_repeat;
			}
			
			bw_freq_OID.write(OID + "/" +OID + "\t" + vw_num + "\t" + visualwords + "\n");
			bw_freq_OID_TW.write(OID + "/" +OID + "\t" + (vw_num + freq_OID_TW.get(OID).split("\\s+").length)+ "\t"
								+ visualwords + " " + freq_OID_TW.get(OID) + "\n");
//			System.out.println("processed: " + visualwords);
		}
		bw_freq_OID.flush();
		bw_freq_OID.close();
		
	}
	

	
	
}
