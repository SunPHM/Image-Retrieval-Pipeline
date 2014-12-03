/*
 * build a randomized kd tree upon and
 * 
 * */
package ir.akm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class RandomizedKDtree{

	
	public static final int partition_size = 5;// the maximum number of points a leaf node can hold
	public Node root =null;
	
	/*
	 * build a randomized kd tree with random num_d dimensions on vector array varray, won't change the varray 
	 * split axis will be chosen from top random num_d dimensions that have the largest variance.s (currently it's not random though)
	 * each level of split dimension will be chosen randomly from those top dimensions
	 * */
	public Node buildTree( int[] dimensions, ArrayList<double[]> varray){
		
		int[] points = new int[varray.size()];
		for(int i = 0; i < varray.size(); i++)
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
	 * 
	 * 
	 * */
	private Node buildKDTree(int[] dim, int level, ArrayList<double[]> varray, int[] points) {
		// TODO Auto-generated method stub
		
		
		//no data points for this node, should be null and return;
		if(points == null || points.length == 0){
			return null;
		}
		
//		System.out.println("points length : " + points.length);
//		for(int point : points){
//			System.out.print("  " + point);
//		}
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
			n.split_axis = dim[new Random().nextInt(dim.length)];
			///n.split_axis = dim[level % dim.length];			
			n.split_value = getExactMedian( varray, points, n.split_axis);
			
			ArrayList<Integer> left_points = new ArrayList<Integer>();
			ArrayList<Integer> right_points = new ArrayList<Integer>();
			ArrayList<Integer> splitting_points = new ArrayList<Integer>();
			
			// divide the points into left child  or right or to the current node itself
			for(int i = 0; i < points.length; i ++){
				//right node
				
				if(varray.get(points[i])[n.split_axis] > n.split_value){
					right_points.add(points[i]);
				}
				//left node
				else if(varray.get(points[i])[n.split_axis] < n.split_value) {
					left_points.add(points[i]);
				}
				else{
					splitting_points.add(points[i]);
				}
			}
//			System.out.println("\t " + left_points.size() + "\t " + right_points.size() + "\t " + splitting_points.size());
		
			int[] left_p = convertIntegers(left_points);
			int[] right_p = convertIntegers(right_points);
			int[] splitting_p = convertIntegers(splitting_points);
			
			n.points = splitting_p;
			
			//build left tree
//			System.out.println("level : " + level);
			n.left = buildKDTree(dim, level + 1, varray, left_p);
			n.right = buildKDTree(dim, level + 1, varray, right_p);
		}
		
		return n;
	}




	/*
	 * get the neareast neighbor Id
	 * */
	public  int getNearestNeighborId(Node root, ArrayList<double[]> varray, double[] q_vector) throws Exception{
		//initialize global variables --  vector[0] as the current nearest neighbor
		NNS nn = new NNS();
		nn.nnId = 0;
		nn.minDistance = getDistance(varray.get(nn.nnId), q_vector);
		nn.comparisons = 0;
		
		nn_recursive(root, varray, q_vector, nn);
		
		System.out.println("comparisons: " + nn.comparisons);
		return nn.nnId;
	};
	//recursive method to get the nearest neighbor Id
	public  void nn_recursive(Node node, ArrayList<double[]> varray, double[] q_vector, NNS nn) throws Exception{
		if(node == null){
			return ;
		}
		
		for(int i : node.points){
			nn.comparisons ++;
			//get the distance of the query vector and the new element in the array
			double dist = getDistance(q_vector, varray.get(i));
			
			if( dist < nn.minDistance){
				nn.nnId = i;
				nn.minDistance = dist;
				System.out.println(nn.nnId + "\t" + nn.minDistance);
			}
		}
		//leaf node case
		if(node.left == null && node.right == null){
			return;
		}
		//internal node
		else {
			// examine if query node position to the hyperplane of the node 
			boolean left_searched = true;
			if(q_vector[node.split_axis] < node.split_value){
				// recursively search the left sub-tree
				nn_recursive(node.left, varray, q_vector, nn);
				left_searched = true;
				
			}
			else {
				nn_recursive(node.right, varray, q_vector, nn);
				left_searched = false;
			}
			//examine the other sub-tree if necessarry
			if(Math.abs(q_vector[node.split_axis] - node.split_value) < nn.minDistance){
				if(left_searched == true){
					nn_recursive(node.right, varray, q_vector, nn);
				}
				else{
					nn_recursive(node.left, varray, q_vector, nn);
				}
			}
			//debugging
			/*else{
				System.out.println("!!!!!!");
			}*/
		}
	}
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
	private double getExactMedian(ArrayList<double[]> varray, int[] points, int split_axis) {
		// TODO Auto-generated method stub
		double[] temp_array = new double[points.length];
		for(int i = 0; i < temp_array.length; i ++){
			temp_array[i] = varray.get(points[i])[split_axis];
		}
		Arrays.sort(temp_array);
		
		return temp_array[(temp_array.length)/2];
	}



	
	//convert ArrayList to int[]
	public static int[] convertIntegers(List<Integer> integers)
	{
	    int[] ret = new int[integers.size()];
	    Iterator<Integer> iterator = integers.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().intValue();
	    }
	    return ret;
	}
	public static void main(String args[]) throws Exception{
		int dim = 100;
		
		Random rand = new Random();
		ArrayList<double[]> varray = new ArrayList<double[]>();
		//double[][] marks={{1.0,2,3,4,5},{10.0,9,8,7,6},{5.0,5,5,5,5}};
		for(int i = 0; i < 10000; i ++){
			double[] arr = new double[dim];
			for (int j = 0; j < arr.length; j ++){
				arr[j] = rand.nextDouble() * (rand.nextInt(100) + 1);
			}
			varray.add(arr);
		}
		
	/*	int[] points =  {0, 1, 2};
		double median = getApproximateMedian(varray, points, 0);
		System.out.println(median);
		*/
		RandomizedKDtree rt = new RandomizedKDtree();
		int[] dims = KDTreeForest.getTopDimensionsWithLargestVariance(10, varray);
		Node root = rt.buildTree(dims, varray);
		
		double[] arr = new double[dim];
		
		for (int j = 0; j < arr.length; j ++){
			arr[j] = rand.nextDouble() * (rand.nextInt(10) + 1);
		}
		
		double[]  q_vector = arr;
		
		long startTime = System.nanoTime();
		int id = rt.getNearestNeighborId(root, varray, q_vector );
		long endTime = System.nanoTime();
		System.out.println("Kdtree nns finished in " + ((double)( endTime - startTime )/(1000 * 1000 * 1000)) + "secs \n distance " + getDistance(q_vector, varray.get(id)));
		
		
		//serial way to find the nearest vector
		long startTime1 = System.nanoTime();
		double min_dist = Double.MAX_VALUE;
		int nnId = -1;
		for(int i = 0; i < varray.size(); i ++){
			double dist = getDistance(varray.get(i), q_vector);
			
			if(dist < min_dist){
				nnId = i;
				min_dist = dist;
			}
		}
		long endTime1 = System.nanoTime();
		System.out.println("serial nns finished in " + ((double)( endTime1 - startTime1)/(1000 * 1000 * 1000)) + "secs ,\n nnID : " +  nnId + "   distance: " + min_dist );
		System.out.println(root.toString() + "   " + id);
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
	
public	int split_axis =0;
public	double split_value = 0;
public	Node left = null;
public	Node right = null;
public	int[] points = null;
	
}