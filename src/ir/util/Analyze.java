package ir.util;

import java.util.ArrayList;
import java.util.List;
import java.io.*;

//code to analyze the output from "hadoop job -list"
//args0: input of recordcontainers.txt
// 
//to be added
//args 1 $FEStart$ 1409027930563
//args 2 $FEEnd$   1409027930563
//args 3 $VWStart$ 1409027930563
//args 4 $VWEnd$   1409027930563
//args 5  $ISStart$ 1409027930563
//args 6 $ISEnd$  1409027930563

public class Analyze {

	public static void main(String args[]){
	//	String logfile = args[0];
		long startTime = Long.parseLong("1413142635609"); 
		long endTime = Long.parseLong("1413154651880");
		String logfile = "AWB_benchmarking_oct_10/ImageNet_1st/ImageNetEvaluate_ImageNet-47GB-456567.seq_topK_100botK_500_botlvlcluster_type_2recordcontainers.txt";
		String result_file = "ImageNetEvaluate_ImageNet-47GB-456567.seq_topK_100botK_500_botlvlcluster_type_2.csv";
		
		
		List<snapshot> snapshots = getSnapshots(logfile);
		WriteResults(result_file,snapshots,startTime,endTime);
		
	}

	private static void WriteResults(String result_file, List<snapshot> snapshots,long startTime,long endTime) {
		// TODO Auto-generated method stub
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(result_file)));
			for(snapshot s:snapshots){
				if(s.timestamp >= startTime && s.timestamp <= endTime){
					String a_record=(s.timestamp-startTime)/1000+","+s.num_containers+","+s.usedmem/1024+"\n";
					System.out.println(a_record);
					bw.write(a_record);
				}
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static List<snapshot> getSnapshots(String logfile) {
		// TODO Auto-generated method stub
		List<snapshot> snapshots = new ArrayList<snapshot>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File(logfile)));
			String line = null;
			long timestamp = 0;
			int totaljobs = 0;
			while((line = br.readLine()) != null){
				if(line.trim().startsWith("TimeStamp")){//possibly a usable snapshot
					timestamp = Long.parseLong((line.split(":"))[1].trim());
					line = br.readLine();
					System.out.println(line);
					totaljobs = Integer.parseInt((line.split(":"))[1].trim());
					if(totaljobs == 0){//no snap shots
						line = br.readLine();
					}
					else{//snapshots, but might not be the one we want, need further test
						line = br.readLine();//read the comment line
						int UsedContainers = 0;
						int UsedMem = 0;
						for(int i = 0; i < totaljobs; i++){
							line = br.readLine();
							String splits[] = line.split("\\s+");
							
							//3 or 4 here!!!??
							int w = 0;
							if(splits[w + 3].trim().equals("ypeng")){
								String s = splits[w + 8].trim();
								s  = s.substring(0, s.length() - 1);
								UsedContainers += Integer.parseInt(splits[w + 6].trim());
								UsedMem += Integer.parseInt(s);
							}
						}
						
						snapshot ss = new snapshot(timestamp, UsedContainers,UsedMem);
						snapshots.add(ss);
						
					}
				}
				
			}
			br.close();
			return snapshots;
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return null;
	}
}

class snapshot{
	long  timestamp;
	//String user;
	int num_containers;
	int usedmem;
	public snapshot(long timestamp, int num_containers, int usedmem){
		this.timestamp = timestamp;
//		this.user=user;
		this.num_containers = num_containers;
		this.usedmem = usedmem;
	}
}