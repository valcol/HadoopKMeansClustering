package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KMapper extends
		Mapper<LongWritable, Text, IntWritable, KValue> {

	 public int k = 0;
	 public Integer[] columns;
	 public int iteration = 1;
	 public String input = "";
	 public String output = "";
	 public Map<Integer, ArrayList<Double>> centroids;
	 
	  @Override
	  public void setup(Context context) throws IOException{
		  
		  //Get job configuration
		  Configuration conf = context.getConfiguration();
		  columns = Arrays.stream(conf.getStrings("columns"))
			.map( s -> Integer.parseInt(s)).toArray(Integer[]::new);
		  k = (int) conf.getInt("k", 10);
		  iteration = conf.getInt("iteration", 1);
		  input = conf.get("input");
		  output = conf.get("output");
		  
		  //Get last iteration finals centroids
		  centroids = KCentroidHelper.get(iteration-1, output);
	  }
	  

	@Override
	public void map(LongWritable key, Text value,
			 Context context) throws IOException, InterruptedException {
		
      ArrayList<Double> coordinates = new ArrayList<Double>();

	  //Split line in an array
	  String line = value.toString();   
	  String[] data = line.split(",");
	  
	  for (int i=0; i<columns.length; i++){
		  coordinates.add(Double.parseDouble(data[columns[i]]));
	  }
	  
	  int NearestCentroid = KCentroidHelper.getNearestCentroid(coordinates, centroids);
	  
	  context.write(new IntWritable(NearestCentroid), new KValue(line, coordinates));

	}

}