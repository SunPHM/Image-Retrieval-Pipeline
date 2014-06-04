package ir.cluster;

import java.io.IOException;

public class BotThread implements Runnable{
	
	String cmd;
	int num;
	
	String cmd0;
	String cmd1;
	String cmd2;
	
	public BotThread(String cmd, int num){
		this.cmd = cmd;
		this.num = num;
	}
	
	public BotThread(String cmd0, String cmd1, String cmd2, int num){
		this.cmd0 = cmd0;
		this.cmd1 = cmd1;
		this.cmd2 = cmd2;
		this.num = num;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		/*Runtime rt = Runtime.getRuntime();
		Process p;
		try {
			p = rt.exec(cmd);
			p.waitFor();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */

		try {
			TopDownClustering.log(cmd0);
			TopDownClustering.run(cmd0);
			TopDownClustering.log(cmd1);
			TopDownClustering.run(cmd1);
			TopDownClustering.log(cmd2);
			TopDownClustering.run(cmd2);
			TopDownClustering.log("thread" + num + "> ends");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
