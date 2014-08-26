package ir.util;
//code to analyze the output from "hadoop job -list"
//args0: input of recordcontainers.txt
// 
//args 1 $FEStart$ 1409027930563
//args 2 $FEEnd$ 1409027930563
//args 3 $VWStart$ 1409027930563
//args 4 $VWEnd$ 1409027930563
//args 5  $ISStart$ 1409027930563
//args 6 $ISEnd$ 1409027930563

public class Analyze {

	public static void main(String args[]){
		
	}
}

class snapshot{
	String timestamp;
	String user;
	int num_containers;
	int usedmem;
	public snapshot(String timestamp,String user, int num_containers, int usedmem){
		this.timestamp=timestamp;
		this.user=user;
		this.num_containers=num_containers;
		this.usedmem=usedmem;
	}
}