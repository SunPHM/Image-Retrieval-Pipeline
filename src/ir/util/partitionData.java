package ir.util;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

//parttion the 47GB data to 5GB, 10GB and 20GB and 30GB filtering out files that does not end with .jpg or .jpeg
//input_folder, output_folder
public class partitionData {
	static final long GB = 1024*1024*1024;
	static int [] sizes = {80, 100, 120, 140, 160};
	private static BytesWritable value=new BytesWritable();
	private static Text key = new Text();
	public static void partition(String args[]) 
			throws IOException, InstantiationException, IllegalAccessException{
		String input = args[0];
		String output = args[1];
		
	//partition the data from 138 seqfile into 20GB, 40GB, 60GB, 80GB, 100GB, 120GB

		//create output dir
		File output_folder = new File(output);
		if(!output_folder.mkdir()){
			System.out.println("failed to create dir: " + output + ", exit");
			return;
		}
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		//create a list of writers to write the output
		String[] outputseqfiles = new String[sizes.length];
		SequenceFile.Writer[] writers = new SequenceFile.Writer[sizes.length];
		for(int i = 0; i < sizes.length; i ++){
			outputseqfiles[i] = output + "/" + sizes[i] + ".seq";
			writers[i] = SequenceFile.createWriter(fs, conf,new Path(outputseqfiles[i]), key.getClass(), value.getClass());
		}
		//output log file
		BufferedWriter  bw = new BufferedWriter(new FileWriter(new File("convert.log")));
		
		//go through all the seqfiles in the input directory
		String tarseqfiles[] = HadoopUtil.getListOfFiles(input);
		
		
		int start_index = 0;
		long bytes_processed = 0;
		int images_processed = 0;
		for(String seqfile : tarseqfiles){
			System.out.println("reading from file: " + seqfile);
			Path path = new Path(seqfile);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			key = (Text) reader.getKeyClass().newInstance();
			value = (BytesWritable) reader.getValueClass().newInstance();
			while (reader.next(key, value)){
				//System.out.println(value.getLength());
				//add this picture to the output list
				for(int i = start_index; i < sizes.length; i ++){
					writers[i].append(key, value);
				}
				
				images_processed ++;
				bytes_processed += value.getLength();
				
				//need to check if reached the limit
				if(bytes_processed > sizes[start_index] * GB){//exceeds the minumum of the start_index file
					writers[start_index].close();
					start_index ++;
					bw.write("finished " + sizes[start_index - 1] + "GB, number of images processed" + images_processed + "\n");
					bw.flush();
					if(start_index == sizes.length){
						reader.close();
						bw.close();
						return;
					}
				}
				//for the 47GB file, stop at 40GB and end this while loop
				if(seqfile.endsWith("ImageNet-47GB-456567.seq")){
					if(bytes_processed >= 40 * GB ){
						reader.close();
						System.out.println("stopping at 40GB of 47GB file");
						break;
					}
				}
				//sleep to avoid 100% use of disk;
				if(images_processed % (5 * 1000) == 0){
					try {
						System.out.println("Sleepting for 1 secs, test info" + key.toString());
						Thread.sleep(1000*1);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			reader.close();
		}
		bw.write("total bytes : " + bytes_processed + ", number of images processed" + images_processed);
		bw.close();
	}
	public static void main(String args[]) throws IOException, InstantiationException, IllegalAccessException{
		
//		args = new String[2];
//		args[0] = "/home/xiaofeng/Downloads/test/test_out" ;
//		args[1] = "/home/xiaofeng/Downloads/test/test_test";
		partition(args);
		

/*		String[] files=HadoopUtil.getListOfFiles(args[0]);
		HashMap<String, Integer> map=new HashMap<String, Integer>();
		int count=0;
		for(String file:files){
			if(file.endsWith(".jpg")==true||file.endsWith(".jpeg")==true
					||file.endsWith(".JPG")==true||file.endsWith(".JPEG")==true){
				
				String key=(new Path(file).getName().split("_"))[0];
				if(map.containsKey(key)==false){//new id add the id
					map.put(key, 1);
					FileUtils.copyFile(new File(file), new File(args[1]+"/"+file));
					count++;
				}
				else{
					if(map.get(key)>=100){
						//do nothing
					}
					else{
						int value=map.get(key)+1;
						map.remove(key);
						map.put(key, value);
						FileUtils.copyFile(new File(file), new File(args[1]+"/"+file));
						count++;
					}
				}
				if(count==500){
					break;
				}
				
			}
		}
		
		
	//	Configuration conf=new Configuration();
		//FileSystem fs =new FileSystem(conf);
	String[] files=HadoopUtil.getListOfFiles(args[0]);
		
		ArrayList<String> allfiles=new ArrayList<String>();
		for(String file:files){
			allfiles.add(file);
		}
		Collections.sort(allfiles);
		Collections.reverse(allfiles);
		List<String> GB5 =new ArrayList<String>();long GB5_size=0;
		List<String> GB10 =new ArrayList<String>();long GB10_size=0;
		List<String> GB20 =new ArrayList<String>();long GB20_size=0;
		List<String> GB30 =new ArrayList<String>();long GB30_size=0;
		long file_count=0;
		for(String file:allfiles){
			if(file.endsWith(".jpg")==true||file.endsWith(".jpeg")==true
					||file.endsWith(".JPG")==true||file.endsWith(".JPEG")==true){
				
				if(GB5_size<=5*GB1){
					GB5.add(file);
					File filepath=new File(file);
					GB5_size=GB5_size+filepath.length();
							
				}
				if(GB10_size<=10*GB1){
					GB10.add(file);
					File filepath=new File(file);
					GB10_size=GB10_size+filepath.length();
							
				}
				if(GB20_size<=20*GB1){
					GB20.add(file);
					File filepath=new File(file);
					GB20_size=GB20_size+filepath.length();
							
				}
				if(GB30_size<=30*GB1){
					GB30.add(file);
					File filepath=new File(file);
					GB30_size=GB30_size+filepath.length();
							
				}
				file_count++;
			}
			
			if(file_count%5000==0){
				System.out.println(file_count+"/"+allfiles.size()+" filenames has been read");
			}
		}
		System.out.println("splitting file names is done, making dirs");
		
//		File output_folder=new File(args[1]);
//		output_folder.mkdir();
		
/*		String str_GB5_folder=args[1]+"/"+"GB5_"+GB5.size();
		File GB5_folder=new File(str_GB5_folder);
		
		String str_GB10_folder=args[1]+"/"+"GB10_"+GB10.size();
		File GB10_folder=new File(str_GB10_folder);
		
		String str_GB20_folder=args[1]+"/"+"GB20_"+GB20.size();
		File GB20_folder=new File(str_GB20_folder);
		
		String str_GB30_folder=args[1]+"/"+"GB30_"+GB30.size();
		File GB30_folder=new File(str_GB30_folder);
		
		
//		GB5_folder.mkdir();
//		GB10_folder.mkdir();
//		GB20_folder.mkdir();
		GB30_folder.mkdir();
		
/*		System.out.println("copying files...\n5GB:");
		for(String file:GB5){
			FileUtils.copyFile(new File(file), new File(str_GB5_folder+"/"+file));
		}
		System.out.println("5 GB output to :"+str_GB5_folder);
		
		System.out.println("copying files...\n10GB:");
		for(String file:GB10){
			FileUtils.copyFile(new File(file), new File(str_GB10_folder+"/"+file));
		}
		System.out.println("10 GB output to :"+str_GB10_folder);
		
		System.out.println("copying files...\n20GB:");
		for(String file:GB20){
			FileUtils.copyFile(new File(file), new File(str_GB20_folder+"/"+file));
		}
		System.out.println("20 GB output to :"+str_GB20_folder);
	
		System.out.println("copying files...\n30GB:");
		for(String file:GB30){
			FileUtils.copyFile(new File(file), new File(str_GB30_folder+"/"+file));
		}
		System.out.println("30 GB output to :"+str_GB30_folder);



		
/*		List<String> GB20_1 =new ArrayList<String>();
		List<String> GB20_2 =new ArrayList<String>();
		List<String> GB20_3 =new ArrayList<String>();
		long curr_size=0;
		for(String file:allfiles){
			if(file.endsWith(".jpg")==true||file.endsWith(".jpeg")==true
					||file.endsWith(".JPG")==true||file.endsWith(".JPEG")==true){
				
				if(curr_size<=20*GB1){
					GB20_1.add(file);
					File filepath=new File(file);
					curr_size=curr_size+filepath.length();
							
				}
				if(curr_size>20*GB1&&curr_size<=40*GB1){
					GB20_2.add(file);
					File filepath=new File(file);
					curr_size=curr_size+filepath.length();
							
				}
				if(curr_size>40*GB1 && curr_size<=60*GB1){
					GB20_3.add(file);
					File filepath=new File(file);
					curr_size=curr_size+filepath.length();
							
				}
			}
			
		}
		File output_folder=new File(args[1]);
		output_folder.mkdir();
		
		String str_1_folder=args[1]+"/"+"GB20_1_"+GB20_1.size();
		File GB20_1_folder=new File(str_1_folder);
		GB20_1_folder.mkdir();
		for(String file:GB20_1){
			FileUtils.copyFile(new File(file), new File(str_1_folder+"/"+file));
		}
		System.out.println("20 GB output to :"+str_1_folder);
		
		
		
		String str_2_folder=args[1]+"/"+"GB20_2_"+GB20_2.size();
		File GB20_2_folder=new File(str_2_folder);
		GB20_2_folder.mkdir();
		for(String file:GB20_2){
			FileUtils.copyFile(new File(file), new File(str_2_folder+"/"+file));
		}
		System.out.println("20 GB output to :"+str_2_folder);
		
		
		String str_3_folder=args[1]+"/"+"GB20_3_"+GB20_3.size();
		File GB20_3_folder=new File(str_3_folder);
		GB20_3_folder.mkdir();
		for(String file:GB20_3){
			FileUtils.copyFile(new File(file), new File(str_3_folder+"/"+file));
		}
		System.out.println("remain GBs output to :"+str_3_folder);
		*/
		
	}
}