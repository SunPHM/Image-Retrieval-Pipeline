package ir.util;

import java.io.IOException;

public class CommandUtil {
	
	public static void runCommand(String cmd){
		Runtime rt = Runtime.getRuntime();
		try {
			rt.exec(cmd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
