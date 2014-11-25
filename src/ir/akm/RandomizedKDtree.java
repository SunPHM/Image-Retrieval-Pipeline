/*
 * build a randomized kd tree upon and
 * 
 * */
package ir.akm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

public class RandomizedKDtree{

	
	public static final int partition_size = 50;// the maximum number of points a leaf node can hold
	public Node root =null;
	

//	private static Vector guess = null;
	
	
	// the axis to split on
	int[] dimensions = null; 
	int K = 0;
	
	/*
	 * build a randomized kd tree with random num_d dimensions on vector array varray, won't change the varray 
	 * split axis will be chosen from top random num_d dimensions that have the largest variance.s (currently it's not random though)
	 * 
	 * */
	public Node buildTree(int num_d, Vector[] varray){
		
		
		long startTime = System.nanoTime();
		//currently build kdtree on dimensions with largest variance, can change this
		dimensions = getTopDimensionsWithLargestVariance(num_d, varray);
		long endTime = System.nanoTime();
		System.out.println("get top variances took " + ((double)( endTime - startTime )/(1000 * 1000 * 1000)) + "secs");
		
		K = dimensions.length;
		
		int[] points = new int[varray.length];
		for(int i = 0; i < varray.length; i++)
			points[i] = i;
		
		long startTime1 = System.nanoTime();
		root = buildKDTree(dimensions, 0,  varray, points);
		long sendTime1 = System.nanoTime();
		System.out.println("building tree took " + ((double)( endTime - startTime )/(1000 * 1000 * 1000)) + "secs");
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
	private Node buildKDTree(int[] dim, int level, Vector[] varray, int[] points) {
		// TODO Auto-generated method stub
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
			n.split_axis = dim[level % dim.length];			
			n.split_value = getApproximateMedian( varray, points, n.split_axis);
			
			ArrayList<Integer> left_points = new ArrayList<Integer>();
			ArrayList<Integer> right_points = new ArrayList<Integer>();
			ArrayList<Integer> splitting_points = new ArrayList<Integer>();
			
			// divide the points into left child  or right or to the current node itself
			for(int i = 0; i < points.length; i ++){
				//right node
				if(varray[points[i]].get(n.split_axis) > n.split_value){
					right_points.add(points[i]);
				}
				//left node
				else if(varray[points[i]].get(n.split_axis) < n.split_value) {
					left_points.add(points[i]);
				}
				else{
					splitting_points.add(points[i]);
				}
			}
			int[] left_p = convertIntegers(left_points);
			int[] right_p = convertIntegers(right_points);
			int[] splitting_p = convertIntegers(splitting_points);
			
			n.points = splitting_p;
			
			//build left tree
			n.left = buildKDTree(dim, level + 1, varray, left_p);
			n.right = buildKDTree(dim, level + 1, varray, right_p);
		}
		
		return n;
	}

	/*
	 * get the neareast neighbor Id
	 * */
	public  int getNearestNeighborId(Node node, Vector[] varray, Vector q_vector) throws Exception{
		//initialize global variables --  vector[0] as the current nearest neighbor
		NNS nn = new NNS();
		nn.nnId = 0;
		nn.minDistance = getDistance(varray[nn.nnId], q_vector);
		nn.comparisons = 0;
		
		nn_recursive(node, varray, q_vector, nn);
		
		System.out.println("comparisons: " + nn.comparisons);
		return nn.nnId;
	};
	//recursive method to get the nearest neighbor Id
	public  void nn_recursive(Node node, Vector[] varray, Vector q_vector, NNS nn) throws Exception{
		if(node == null){
			return ;
		}
		
		for(int i : node.points){
			nn.comparisons ++;
			//get the distance of the query vector and the new element in the array
			double dist = getDistance(q_vector, varray[i], dimensions);
			
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
			if(q_vector.get(node.split_axis) < node.split_value){
				// recursively search the left sub-tree
				nn_recursive(node.left, varray, q_vector, nn);
				left_searched = true;
				
			}
			else {
				nn_recursive(node.right, varray, q_vector, nn);
				left_searched = false;
			}
			//examine the other sub-tree if necessarry
			if(Math.abs(q_vector.get(node.split_axis) - node.split_value) < nn.minDistance){
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
	private static double getDistance(Vector v1, Vector v2) throws Exception {
		// TODO Auto-generated method stub
		if(v1 == null || v2 == null){
			throw new Exception("null node distance error");
			
		}
		//get eclidean distance
		double sum = 0;
		return Math.sqrt(v1.getDistanceSquared(v2));
		
	}
	//get the distance of the specified dimensions only
	private static double getDistance(Vector v1, Vector v2, int[] dims) throws Exception {
		// TODO Auto-generated method stub
		if(v1 == null || v2 == null){
			throw new Exception("null node distance error");
			
		}
		//get eclidean distance
		double sum = 0;
		for(int i : dims){
			sum = sum + (v1.get(i) - v2.get(i)) * (v1.get(i) - v2.get(i));
		}
		return Math.sqrt(sum);
	}



	//get the approximate median point by getting the median of the median of the 5-element sub arrays
	private static double getApproximateMedian(Vector[] varray, int[] points, int split_axis) {
		// TODO Auto-generated method stub
		int k = (int) Math.ceil((double) points.length / 5);
		ArrayList<Double> medians = new ArrayList<Double>();
		
		ArrayList<Double> find_m = new ArrayList<Double>();
		for (int i = 0; i < k; i ++){
			//find the median in the 5-element array
			find_m.clear();
			for (int j = i * 5; j < points.length && j < i * 5 + 5; j++){
				find_m.add( varray[points[j]].get(split_axis) );
			}
			Collections.sort(find_m);
			double median = find_m.get( (find_m.size())/ 2 );
			medians.add(median);
		}
		
		Collections.sort(medians);
		return medians.get( (medians.size())/ 2 );
		
		
	}


	//get the top num_d dimensions with the largest variance for splitting the space
	private static int[] getTopDimensionsWithLargestVariance(int num_d, Vector[] varray) {
		// TODO Auto-generated method stub
		int[] dims = new int[num_d];
		double[] variance_num_d = new double[num_d];
		
		double[] all_variances = new double[varray[0].size()];
		
		for(int k = 0; k < num_d; k ++){
			variance_num_d[k] = 0;
		}
		
		for(int i = 0; i < varray[0].size(); i ++){
			
			//calc variance for dim = i
			double variance = 0;
			double mean = 0;
			for(int j = 0; j < varray.length; j++){
				variance = variance + varray[j].get(i) * varray[j].get(i);
				mean = mean + varray[j].get(i);
			}
			mean = mean / varray.length;
			variance = variance - varray.length * mean * mean;
			
			all_variances[i] = variance;
			//update the dims array
			int k = 0;
			for(k = 0 ; k < dims.length; k ++){
				if(variance > variance_num_d[k])
					break;
			}
			for(int s = dims.length - 1; s > k; s --){
				dims[s] = dims[s - 1];
				variance_num_d[s] = variance_num_d[s - 1];
			}
			if(k < dims.length){
				dims[k] = i;
				variance_num_d[k] = variance;
			}
			
		}
		
		
		return dims;
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
		int dim = 120;
		
		Random rand = new Random();
		Vector[] varray = new Vector[1000000];
		//double[][] marks={{1.0,2,3,4,5},{10.0,9,8,7,6},{5.0,5,5,5,5}};
		for(int i = 0; i < varray.length; i ++){
			double[] arr = new double[dim];
			for (int j = 0; j < arr.length; j ++){
				arr[j] = rand.nextDouble() * (rand.nextInt(100) + 1);
			}
			varray[i] = new DenseVector(arr);
		}
		
	/*	int[] points =  {0, 1, 2};
		double median = getApproximateMedian(varray, points, 0);
		System.out.println(median);
		*/
		RandomizedKDtree rt = new RandomizedKDtree();
		Node root = rt.buildTree(120, varray);
		
		double[] arr = new double[dim];
		
		for (int j = 0; j < arr.length; j ++){
			arr[j] = rand.nextDouble() * (rand.nextInt(10) + 1);
		}
		
		Vector q_vector = new DenseVector(arr);
		
		long startTime = System.nanoTime();
		int id = rt.getNearestNeighborId(root, varray, q_vector );
		long endTime = System.nanoTime();
		System.out.println("Kdtree nns finished in " + ((double)( endTime - startTime )/(1000 * 1000 * 1000)) + "secs \n distance " + getDistance(q_vector, varray[id]));
		
		
		//serial way to find the nearest vector
		long startTime1 = System.nanoTime();
		double min_dist = Double.MAX_VALUE;
		int nnId = -1;
		for(int i = 0; i < varray.length; i ++){
			double dist = getDistance(varray[i], q_vector);
			
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