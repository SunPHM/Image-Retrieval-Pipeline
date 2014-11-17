package ir.rerank;

public class Reranking {
	
	public static void main(String[] args){
		
	}
	
	public static void test(){
		
	}
	
	public static double tfidfSimilarity(double[] a, double[] b){
		return 0;
	} // how to get the tf-idf vectors
	
	public static double[] stringToDoubleArray(String s, int length){
		double[] histogram = new double[length];
		String[] sa = s.split(" ");
		for(int i = 0; i < sa.length; i++){
			int x = Integer.parseInt(sa[i]);
			histogram[x] = histogram[x] + 1;
		}
		return histogram;
	}
	
	
	public static double computeSimilarity(double[] a, double[] b, int normType, int similarityType){
		// preprocess the double arrays according to normalization type
		double[] aa = new double[a.length]; 
		double[] bb = new double[b.length];
		if (normType == 0) { // raw vectors
			aa = a;
			bb = b;
		}
		if (normType == 1){ // l1 norm
			double anorm = l1Norm(a);
			double bnorm = l1Norm(b);
			for(int i = 0; i < a.length; i++){
				aa[i] = a[i] / anorm;
				bb[i] = b[i] / bnorm;
			}
		}
		if (normType == 2){ // l2 norm
			double anorm = l2Norm(a);
			double bnorm = l2Norm(b);
			for(int i = 0; i < a.length; i++){
				aa[i] = a[i] / anorm;
				bb[i] = b[i] / bnorm;
			}
		}
		
		// calculating similarity according to similarity type
		double distance = 0;
		if (similarityType == 0){ // cosine distance
			distance = cosineSimilarity(aa, bb);
		}
		if (similarityType == 1){ // l1 norm
			double[] cc = new double[aa.length];
			for (int j = 0; j < aa.length; j++) cc[j] = aa[j] - bb[j];
			distance = l1Norm(cc);
		}
		if (similarityType == 2){
			double[] cc = new double[aa.length];
			for (int j = 0; j < aa.length; j++) cc[j] = aa[j] - bb[j];
			distance = l2Norm(cc);
		}
		return distance;
	}
	
	public static double cosineSimilarity (double[] a, double[] b){
		double distance = 0;
		double ds = 0, an = 0, bn = 0;
		for(int j = 0; j < a.length; j++){
			ds += a[j] * b[j];
			an += a[j] * a[j];
			bn += b[j] * b[j];
		}
		distance = - ds / (Math.sqrt(an) * Math.sqrt(bn));
		return distance;
	}
	
	public static double l1Norm (double[] a){
		double distance = 0;
		for(int i = 0; i < a.length; i++){
			distance += Math.abs(a[i]);
		}
		return distance;
	}
	
	public static double l2Norm (double[] a){
		double distance = 0;
		for(int i = 0; i < a.length; i++){
			distance += Math.sqrt(Math.pow(a[i], 2));
		}
		return distance;
	}
}

