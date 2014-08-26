package ir.util;

public class testMeasureTime {

	public static void main(String[] args) throws InterruptedException{
		MeasureContainers mt=new MeasureContainers("testMT.txt");
		mt.run();
	//	Thread.sleep(3000);
		mt.stopMe();
	}
}
