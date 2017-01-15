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
	
	public static Map<String, Map<Integer, ArrayList<Double>>> get(int iteration, int depth, String output) throws IOException{
		
		Map<String, Map<Integer, ArrayList<Double>>> centroids = new HashMap<String, Map<Integer,ArrayList<Double>>>();
		Configuration conf = new Configuration();
		FileSystem fs = new Path(output).getFileSystem(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(output+"/centroids/c"+iteration+".csv"))));
		try {
		  String line;
		  line=br.readLine();
		  while (line != null){
			String[] lineSplit =  line.split(",");
			String centroidKey = String.join(",", Arrays.copyOfRange(lineSplit, 0, depth));
			int centroidK = Integer.valueOf(lineSplit[depth]);
			Double[] centroidsDots = Arrays.stream(Arrays.copyOfRange(lineSplit, depth+1, lineSplit.length))
					.map( s -> Double.parseDouble(s)).toArray(Double[]::new); 
			
			centroids.computeIfPresent(centroidKey, (k, v) -> {
				Map<Integer, ArrayList<Double>> v2 = v;
				v2.put(centroidK, new ArrayList<Double>(Arrays.asList(centroidsDots)));
				return v;
			});
			
			centroids.computeIfAbsent(centroidKey, (v) -> {
				Map<Integer, ArrayList<Double>> v2 = new HashMap<Integer,ArrayList<Double>>(); 
				v2.put(centroidK, new ArrayList<Double>(Arrays.asList(centroidsDots)));
				return v2;
			});
			
			line = br.readLine(); 
		  }
		} finally {
		  br.close();
		}
		
		return centroids;
		
	}
	
	
	public static void writeToFile(int iteration, String output, ArrayList<Double> centroid, String centroidkey) throws IllegalArgumentException, IOException{
		
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
		tmp.append(centroidkey);
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
	
	public static int getNearestCentroid(ArrayList<Double> coordinates, Map<Integer, ArrayList<Double>> map){
		
		double minDist = Double.MAX_VALUE;
		int nearestCentroid = 0;
		for (int key : map.keySet()){
			double distance = EuclidianDistance(coordinates, map.get(key));
			if (distance < minDist){
				minDist = distance;
				nearestCentroid = key;
			}
		}
		
		return nearestCentroid;
	}
	
	public static boolean compareCentroids(int iteration, int depth, String output, double criterionValue) throws IOException{
		
	    boolean stopCritIsMet = true; 
	    Map<String, Map<Integer, ArrayList<Double>>> centroidsA = get(iteration-1, depth, output);
	    Map<String, Map<Integer, ArrayList<Double>>> centroidsB = get(iteration, depth, output);
	    
	    for (String key : centroidsA.keySet()){
	    	for (Integer k : centroidsA.get(key).keySet()){
			    double diff = KCentroidHelper.EuclidianDistance(centroidsA.get(key).get(k), centroidsB.get(key).get(k));
			    if (diff > criterionValue)
			    	stopCritIsMet = false; 
	    	}
	    }
	    
		return stopCritIsMet;
	    
	}

}