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

public class KReducer extends Reducer<KKeyWritable,KValueWritable,Text,Text> {
	
	 public int k;
	 public Integer[] columns;
	 public int currentIteration;
	 public boolean lastIteration = false; 
	 public MultipleOutputs<Text, Text> mos;

	 
	  @Override
	  public void setup(Context context) throws IOException{
		  
		  //get job configuration
		  Configuration conf = context.getConfiguration();
		  columns = Arrays.stream(conf.getStrings("columns"))
			.map( s -> Integer.parseInt(s)).toArray(Integer[]::new);
		  k = (int) conf.getInt("k", 10);
		  currentIteration = conf.getInt("currentIteration", 0);
		  lastIteration = conf.getBoolean("lastIteration", false);
		  mos = new MultipleOutputs(context);

	  }
	
	@Override 
    public void reduce(KKeyWritable key, Iterable<KValueWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
		
		//if this is not the first iteration 
		if (currentIteration > 0){
			Double[] sum = new Double[columns.length];
			int numberOfValues = 0;
			String currentLabelValue = "";
			double currentMesureValue = Double.MIN_VALUE;
			
			//init the sum array
			for (int i=0; i<sum.length; i++){
				sum[i]=0.0;
			}
	    	
			for (KValueWritable value : values) {
				
				//get the label with the greater mesure
				if (currentMesureValue < value.getMesure()){
					currentMesureValue = value.getMesure();
					currentLabelValue = value.getLabel();
				}
				
				//if this is the last iteration, write data
				if (lastIteration){
					mos.write("data", new Text(key.toString()), new Text(","+value.getContent()));
				}
				else{
					ArrayList<Double> coordinates = value.getCoordinates();
					//sum the coordinates
					for (int i=0; i<coordinates.size(); i++){
						sum[i]+=coordinates.get(i);
					}
					numberOfValues++;
				}
				
			}
			
			//if this is the last iteration, write label
			if (lastIteration)
				mos.write("labels", new Text(key.toString()), new Text(","+currentLabelValue+","+currentMesureValue));
			else {
				//compute the avg of each coordinates
				for (int i=0; i<sum.length; i++){
					sum[i]/=numberOfValues;
				}
				
				//write the new centroid
				String newCentroids =  String.join(",", Arrays.stream(sum)
						.map( i -> String.valueOf(i)).toArray(String[]::new));
				
				mos.write("centroids", new Text(key.toString()), new Text(","+newCentroids));
			}
		}
		else {
			int i = 0;
			//take the first k values as centroids
			for (KValueWritable value : values) {
				if (i<k+1){
					key.setK(i);
					mos.write("centroids", new Text(key.toString()), new Text(","+value.coordinatesToString()));
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
