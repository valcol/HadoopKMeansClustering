package kmeans;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

public class KCentroidHelper {
	
	public static void init(String input, String output, Integer[] columns, int k) throws IOException{
		
		Map<Integer, ArrayList<Double>> centroids = new HashMap<Integer,ArrayList<Double>>();
		Configuration conf = new Configuration();
		FileSystem fs = new Path(input).getFileSystem(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(input))));
		try {
		  String line;
		  line=br.readLine();
		  int n = 0;
		  while (line != null && n<k){
			 ArrayList<Double> centroid = new ArrayList<Double>();
			 boolean failed = false;
			 for (int column : columns){
				double colValue = 0;
				try {
					colValue = Double.parseDouble(line.split(",")[column]);
					centroid.add(colValue);
				  }
				  catch (NumberFormatException nfe){
					  failed = true;
				  }
			 }
			  
			if (!failed){
				centroids.put(n, centroid);
				n++;
			}
				
		    line = br.readLine();
		  }
		} finally {
		  br.close();
		}
		
	    for (int i = 0; i < centroids.size(); i++){
	    	KCentroidHelper.writeToFile(0, output, centroids.get(i), i);
	    }
		
	}
	
	public static void sentToCache(int iteration, Path path){
		//TODO
	}
	
	public static Map<Integer, ArrayList<Double>> get(int iteration, String output) throws IOException{
		
		Map<Integer, ArrayList<Double>> centroids = new HashMap<Integer,ArrayList<Double>>();
		Configuration conf = new Configuration();
		FileSystem fs = new Path(output).getFileSystem(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(output+"/centroids/c"+iteration+".csv"))));
		try {
		  String line;
		  line=br.readLine();
		  while (line != null){
			Double[] centroidsStrings = Arrays.stream(line.split(","))
					.map( s -> Double.parseDouble(s)).toArray(Double[]::new); 
			centroids.put(centroidsStrings[0].intValue(), new ArrayList<Double>(Arrays.asList(Arrays.copyOfRange(centroidsStrings, 1, centroidsStrings.length))));
		    line = br.readLine();
		  }
		} finally {
		  br.close();
		}
		
		return centroids;
		
	}
	
	
	public static void writeToFile(int iteration, String output, ArrayList<Double> centroid, int centroidNumber) throws IllegalArgumentException, IOException{
		
		Configuration conf = new Configuration();
		FileSystem fs = new Path(output).getFileSystem(conf);
		Path outputPath = new Path(output+"/centroids/c"+iteration+".csv"); 
		OutputStream os;
		if (fs.exists(outputPath)) 
			os = fs.append(outputPath);
		else
			os = fs.create(outputPath);
		PrintStream ps = new PrintStream(os); 
		StringBuffer tmp = new StringBuffer();
		tmp.append(centroidNumber);
		tmp.append(',');
		for (int j = 0; j < centroid.size(); j++){
			tmp.append(centroid.get(j));
			if (j+1 < centroid.size())
				tmp.append(',');
			else
				tmp.append(System.getProperty("line.separator"));
			
		}
		ps.print(tmp.toString());		
		os.close();
		
	}

	public static double EuclidianDistance(ArrayList<Double> centroidA, ArrayList<Double> centroidB) {
		double d = 0;
		for (int i = 0; i < centroidA.size(); i++){
			d+=Math.pow(centroidA.get(i)-centroidB.get(i), 2);
		}
		return Math.sqrt(d);
	}
	
	public static int getNearestCentroid(ArrayList<Double> coordinates, Map<Integer, ArrayList<Double>> centroids){
		
		double minDist = Double.MAX_VALUE;
		int nearestCentroid = 0;
		for (int i = 0; i < centroids.size(); i++){
			double distance = EuclidianDistance(coordinates, centroids.get(i));
			if (distance < minDist){
				minDist = distance;
				nearestCentroid = i;
			}
		}
		
		return nearestCentroid;
	}
	
	public static boolean compareCentroids(int iteration, String output, double criterionValue) throws IOException{
		
	    boolean stopCritIsMet = true; 
	    Map<Integer, ArrayList<Double>> centroidsA = get(iteration-1, output);
	    Map<Integer, ArrayList<Double>> centroidsB = get(iteration, output);
	    
	    for (int i=0; i<centroidsA.size(); i++){
		    double diff = KCentroidHelper.EuclidianDistance(centroidsA.get(i), centroidsB.get(i));
		    if (diff > criterionValue)
		    	stopCritIsMet = false; 
	    }
	    
		return stopCritIsMet;
	    
	}

}