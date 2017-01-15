package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class KReducer extends Reducer<KDoubleArrayWritable,KValueWritable,Text,Text> {
	
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

	 
	  @Override
	  public void setup(Context context) throws IOException{
		  
		  //Get job configuration
		  Configuration conf = context.getConfiguration();
		  columns = Arrays.stream(conf.getStrings("columns"))
			.map( s -> Integer.parseInt(s)).toArray(Integer[]::new);
		  k = (int) conf.getInt("k", 10);
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
    public void reduce(KDoubleArrayWritable key, Iterable<KValueWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
		
		if (currentIteration > 0){
			Double[] sum = new Double[columns.length];
			int numberOfValues = 0;
			
			for (int i=0; i<sum.length; i++){
				sum[i]=0.0;
			}
	    	
			for (KValueWritable value : values) {
				ArrayList<Double> coordinates = value.getCoordinates();
				for (int i=0; i<coordinates.size(); i++){
					sum[i]+=coordinates.get(i);
				}
				numberOfValues++;
				
				if (lastIteration){
					context.write(new Text(key.toString()), new Text(","+value.getContent()));
				}
			}
			
			for (int i=0; i<sum.length; i++){
				sum[i]/=numberOfValues;
			}
			
			KCentroidHelper.writeToFile(currentIteration, output, new ArrayList<Double>(Arrays.asList(sum)), key.toString());
		}
		else {
			int i = 0;
			for (KValueWritable value : values) {
				ArrayList<Double> coordinates = value.getCoordinates();
				if (i<k+1){
					key.setK(i);
					KCentroidHelper.writeToFile(currentIteration, output, new ArrayList<Double>(coordinates), key.toString());
				}
				i++;
			}
		}
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

}
