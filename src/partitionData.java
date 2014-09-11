import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import ir.util.HadoopUtil;

//parttion the 47GB data to 5GB, 10GB and 20GB and 30GB filtering out files that does not end with .jpg or .jpeg
//input_folder, output_folder
public class partitionData {
	static long GB1=1024*1024*1024;
	
	
	public static void main(String args[]) throws IOException{
		String[] files=HadoopUtil.getListOfFiles(args[0]);
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
/*		String[] files=HadoopUtil.getListOfFiles(args[0]);
		
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