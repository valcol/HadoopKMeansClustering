package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends
		Mapper<LongWritable, Text, KKeyWritable, KValueWritable> {

	 public Integer[] columns;
	 public int currentIteration;
	 public int currentDepth;
	 public Map<String, Map<Integer, ArrayList<Double>>> centroids;
	 public int mesureCol;
	 public int labelCol;
	 public String output;

	 
	  @Override
	  public void setup(Context context) throws IOException{
		  
		  //Get job configuration
		  Configuration conf = context.getConfiguration();
		  columns = Arrays.stream(conf.getStrings("columns"))
			.map( s -> Integer.parseInt(s)).toArray(Integer[]::new);
		  mesureCol = conf.getInt("mesureCol", 0);
		  labelCol = conf.getInt("labelCol", 0);
		  currentIteration = conf.getInt("currentIteration", 0);
		  currentDepth = conf.getInt("currentDepth", 0);
		  output = conf.get("output");
		  //Get previous iteration finals centroids
		  if (currentIteration > 0)
			  centroids = KCentroidHelper.get(currentIteration-1, currentDepth, output);
	  }
	  

	@Override
	public void map(LongWritable key, Text value,
			 Context context) throws IOException, InterruptedException {
	
      ArrayList<Double> coordinates = new ArrayList<Double>();

      //Split the line using ',' as separator 
	  String line = value.toString();
	  String[] lineSplitted = line.split(",");
	  
	  //Separates the clusterId and the line data
	  String clusterId = String.join(",", Arrays.copyOfRange(lineSplitted, 0, currentDepth));
	  String data = String.join(",", Arrays.copyOfRange(lineSplitted, currentDepth, lineSplitted.length));
	  //Get the label
	  String label = lineSplitted[labelCol+currentDepth];
	  double mesure;
	  
	  try{
		  //Get the mesure
		  mesure = Double.parseDouble(lineSplitted[mesureCol+currentDepth]);
		  //Gte the coordinates
		  for (int i=0; i<columns.length; i++){
			  coordinates.add(Double.parseDouble(lineSplitted[columns[i]+currentDepth]));
		  }
		  
		  int nearestCentroid = 0;
		  
		  //If this is not the first iteration (i.e previous centroids exists), get the nearest centroid
		  if (currentIteration>0)
			  nearestCentroid = KCentroidHelper.getNearestCentroid(coordinates, centroids.get(clusterId));
		  
		  context.write(new KKeyWritable(clusterId, nearestCentroid), new KValueWritable(data, label, mesure, coordinates));
	  }
	  catch(NumberFormatException e){
	       System.out.println("Error parsing number, ignoring line..");
	  }

	}

}