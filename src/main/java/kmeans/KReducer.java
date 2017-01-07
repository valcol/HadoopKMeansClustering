package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class KReducer extends Reducer<IntWritable,KValue,IntWritable,Text> {
	
	public int k = 0;
	 public Integer[] columns;
	 public int iteration = 1;
	 public String input = "";
	 public String output = "";
	 public Map<Integer, ArrayList<Double>> centroids;
	 public boolean lastIteration = false; 
	 public MultipleOutputs<Text, Text> mos;

	 
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
		  lastIteration = conf.getBoolean("lastIteration", false);
		  mos = new MultipleOutputs(context);
		  //Get previous iteration finals centroids
		  centroids = KCentroidHelper.get(iteration-1, output);
	  }
	
	@Override 
    public void reduce(IntWritable key, Iterable<KValue> values,
                       Context context
                       ) throws IOException, InterruptedException {
		
		Double[] sum = new Double[columns.length];
		int numberOfValues = 0;
		
		for (int i=0; i<sum.length; i++){
			sum[i]=0.0;
		}
    	
		for (KValue value : values) {
			ArrayList<Double> coordinates = value.getCoordinates();
			for (int i=0; i<coordinates.size(); i++){
				sum[i]+=coordinates.get(i);
			}
			numberOfValues++;
			
			if (lastIteration){
	            mos.write(key.get()+"", new IntWritable(key.get()), new Text(","+value.getContent()));
			}
		}
		
		for (int i=0; i<sum.length; i++){
			sum[i]/=numberOfValues;
		}
		
		KCentroidHelper.writeToFile(iteration, output, new ArrayList<Double>(Arrays.asList(sum)), key.get());
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

}
