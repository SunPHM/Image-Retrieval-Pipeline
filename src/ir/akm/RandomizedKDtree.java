/*
 * build a randomized kd tree upon and
 * 
 * */
package ir.akm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class RandomizedKDtree{

	
	public static final int partition_size = 10;// the maximum number of points a leaf node can hold
	public Node root =null;
	
	/*
	 * build a randomized kd tree with random num_d dimensions on vector array varray, won't change the varray 
	 * split axis will be chosen from top random num_d dimensions that have the largest variance.s (currently it's not random though)
	 * each level of split dimension will be chosen randomly from those top dimensions
	 * */
	public Node buildTree( int[] dimensions, double[][] varray){
		
		int[] points = new int[varray.length];
		for(int i = 0; i < varray.length; i++)
			points[i] = i;
		
		long startTime1 = System.nanoTime();
		root = buildKDTree(dimensions, 0,  varray, points);
		long endTime1 = System.nanoTime();
		System.out.println("building tree took " + ((double)( endTime1 - startTime1 )/(1000 * 1000 * 1000)) + "secs");
		return root;
	}

	
	
	/*
	 * recursively build kd tree on  sub array of varray, specified by points
	 * each node will have no more than partition_size number of points
	 * 
	 * @param dim : the dimensions to split on
	 * @param level: the recusive level, used to decide which dim to split on.
	 * @param varry: the array to build kdtree upon
	 * @param points: list of integers indicate the vectors in varray that we need to build kdtree on
	 * 
	 * internal nodes should not contain actual points though
	 * 
	 * */
	private Node buildKDTree(int[] dim, int level, double[][] varray, int[] points) {
		// TODO Auto-generated method stub
		
		
		//no data points for this node, should be null and return;
		if(points == null || points.length == 0){
			return null;
		}
		
		Node n = new Node();
		
		// if poins.length <= partitions_size, reach leaf node
		if(points.length <= partition_size){
			n.points = points;
			n.left = null;
			n.right = null;
			n.split_axis = -1;
			n.split_value = Integer.MIN_VALUE;
		}
		//else need to split further into smaller partitions
		else{
			
			//choose split axis randomly here
			int s_a = new Random().nextInt(dim.length);
			n.split_axis = dim[s_a];
//			System.out.println("split axis " + s_a);
			
			/*TODO change here to decide split on median or mean
			 * */
			/*use median for splitting	
			 */
			n.split_value = getExactMedian( varray, points, n.split_axis);
			
			/*test use mean as splitting value
			 */
		//	n.split_value = getMean( varray, points, n.split_axis);
			
			//get count of points not greater than the split value
			int num_not_greater_than_median = 0;
			for(int point : points){
				if(varray[point][n.split_axis] <= n.split_value){
					num_not_greater_than_median ++;
				}
					
			}
			int[] left_p = null;
			int[] right_p = null;
			// ----rare possibility that num_not_greater_than_median = points.length, i.e. all data points not greater than median
			///to prevent infinite recursion, 
			// need to force split both array into at least one element
			if(num_not_greater_than_median == points.length){
				boolean filled = false;
				 left_p = new int[points.length - 1];
				 right_p = new int[1];
				 // iterate thought the points, and assign just one element equal to median to right array, the rest to left array
				for(int i = 0; i < points.length; i ++){
					if(filled == false && varray[points[i]][n.split_axis] == n.split_value ){
						right_p[0] = points[i];
						filled = true;
					}
					else{
						int k = i;
						if(filled == true){
							k--;
						}
						left_p[k] = points[k];
					}
				}
			}
			//normal case where left_p and right_p would both have elements
			else{

				left_p = new int[num_not_greater_than_median];
				right_p = new int[points.length - num_not_greater_than_median];
				int left_i = 0;
				int right_i = 0;
				for(int point : points){
					if(varray[point][n.split_axis] <= n.split_value){
						left_p[left_i ++] = point;
					}
					else{
						right_p[right_i ++] = point;
					}
						
				}
			}
			int[] dim_left = KDTreeForest.getTopDimensionsWithLargestVariance(5, left_p, varray);
			int[] dim_right = KDTreeForest.getTopDimensionsWithLargestVariance(5, right_p, varray);
			//recursive build the left tree and right tree
			n.left = buildKDTree(dim_left, level + 1, varray, left_p);
			n.right = buildKDTree(dim_right, level + 1, varray, right_p);
			
		}
		
		return n;
	}

	// get the mean of points in the varray of the split_axis
	private double getMean(ArrayList<double[]> varray, int[] points, int split_axis) {
		// TODO Auto-generated method stub
		double sum = 0;
		for(int i = 0; i < points.length; i ++){
			sum += varray.get(points[i])[split_axis];
		}
		return sum / points.length;
		
	}
	
	//get the Euclidean distance of two vectors
	static double getDistance(double[] v1, double[] v2) throws Exception {
		// TODO Auto-generated method stub
		if(v1 == null || v2 == null){
			throw new Exception("null node distance error");
			
		}
		else if (v1.length != v2.length){
			throw new Exception("vector not of the same size distance error");
		}
		//get eclidean distance
		double sum = 0;
		for(int i = 0; i < v1.length; i ++){
			sum = sum + (v1[i] - v2[i]) * (v1[i] - v2[i]);
		}
		return Math.sqrt(sum);
	}

	//get the approximate median point by getting the median of the median of the 5-element sub arrays
	private static double getApproximateMedian(ArrayList<double[]> varray, int[] points, int split_axis) {
		// TODO Auto-generated method stub
		int k = (int) Math.ceil((double) points.length / 5);
		ArrayList<Double> medians = new ArrayList<Double>();
		
		ArrayList<Double> find_m = new ArrayList<Double>();
		for (int i = 0; i < k; i ++){
			//find the median in the 5-element array
			find_m.clear();
			for (int j = i * 5; j < points.length && j < i * 5 + 5; j++){
				find_m.add( varray.get(points[j])[split_axis] );
			}
			Collections.sort(find_m);
			double median = find_m.get( (find_m.size())/ 2 );
			medians.add(median);
		}
		
		Collections.sort(medians);
		return medians.get( (medians.size())/ 2 );
		
		
	}
	private double getExactMedian(double[][] varray, int[] points, int split_axis) {
		// TODO Auto-generated method stub
		double[] temp_array = new double[points.length];
		for(int i = 0; i < temp_array.length; i ++){
			temp_array[i] = varray[points[i]][split_axis];
		}
		Arrays.sort(temp_array);
		
		return temp_array[(temp_array.length)/2];
	}

	public static void main(String args[]) throws Exception{
		
	}
}
//used for neareast neigbor search -- used as "global variables" to store the currently nearest neighbor found
class NNS{
	public  double minDistance = Double.MAX_VALUE;
	public  int nnId = -1;
	
	//test to check the number of comparisons 
	public int comparisons = 0;
}


class Node {
	
public	int split_axis =-1;
public	double split_value = Double.MIN_VALUE;
public	Node left = null;
public	Node right = null;
public	int[] points = null;
	
}
class Index_Value{
	public int index;
	public double value;
	public Index_Value(int i, double v){
		this.index = i;
		this.value = v;
	}
}
