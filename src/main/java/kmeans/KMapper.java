package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class KMapper extends
		Mapper<LongWritable, Text, KDoubleArrayWritable, KValueWritable> {

	 public int k;
	 public Integer[] columns;
	 public int currentIteration;
	 public int currentDepth;
	 public String input;
	 public String output;
	 public Map<String, Map<Integer, ArrayList<Double>>> centroids;
	 public boolean lastIteration = false; 
	 public boolean isMaxDepth = false;
	 public MultipleOutputs<Text, Text> mos;
	 public int mesureCol;
	 public int labelCol;

	 
	  @Override
	  public void setup(Context context) throws IOException{
		  
		  //Get job configuration
		  Configuration conf = context.getConfiguration();
		  columns = Arrays.stream(conf.getStrings("columns"))
			.map( s -> Integer.parseInt(s)).toArray(Integer[]::new);
		  k = (int) conf.getInt("k", 10);
		  mesureCol = (int) conf.getInt("mesureCol", 0);
		  labelCol = (int) conf.getInt("labelCol", 0);
		  currentIteration = conf.getInt("currentIteration", 0);
		  currentDepth = conf.getInt("currentDepth", 0);
		  input = conf.get("input");
		  output = conf.get("output");
		  lastIteration = conf.getBoolean("lastIteration", false);
		  isMaxDepth = conf.getBoolean("isMaxDepth", false);
		  mos = new MultipleOutputs(context);
		  //Get previous iteration finals centroids
		  if (currentIteration > 0)
			  centroids = KCentroidHelper.get(currentIteration-1, currentDepth, output);
	  }
	  

	@Override
	public void map(LongWritable key, Text value,
			 Context context) throws IOException, InterruptedException {
		
      ArrayList<Double> coordinates = new ArrayList<Double>();

	  //Split line in an array
	  String line = value.toString();
	  String[] lineSplitted = line.split(",");
	  
	  String centroidKey = String.join(",", Arrays.copyOfRange(lineSplitted, 0, currentDepth));
	  String data = String.join(",", Arrays.copyOfRange(lineSplitted, currentDepth, lineSplitted.length));
	  String label = lineSplitted[labelCol+currentDepth];
	  double mesure = Double.parseDouble(lineSplitted[mesureCol+currentDepth]);
	  
	  for (int i=0; i<columns.length; i++){
		  coordinates.add(Double.parseDouble(lineSplitted[columns[i]+currentDepth]));
	  }
	  
	  int nearestCentroid = 0;
	  
	  if (currentIteration>0)
		  nearestCentroid = KCentroidHelper.getNearestCentroid(coordinates, centroids.get(centroidKey));
	  
	  context.write(new KDoubleArrayWritable(centroidKey, nearestCentroid), new KValueWritable(data, label, mesure, coordinates));

	}

}