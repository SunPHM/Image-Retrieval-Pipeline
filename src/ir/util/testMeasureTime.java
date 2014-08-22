package ir.util;

public class testMeasureTime {

	public static void main(String[] args) throws InterruptedException{
		MeasureTime mt=new MeasureTime("testMT.txt");
		mt.run();
	//	Thread.sleep(3000);
		mt.stopMe();
	}
}
