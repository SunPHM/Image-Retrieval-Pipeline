package ir.index;

public class F1Score {
	double precision;
	double recall;
	double F1;
	F1Score(double pre, double re){
		precision = pre;
		recall = re;
		F1 = 2 * (precision * recall) / (precision + recall);
	}
	public double getRec(){
		return this.recall;
	}
	public double getPre(){
		return this.precision;
	}
}