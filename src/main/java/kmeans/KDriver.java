package kmeans;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class KDriver {
	
	public static String input;
	public static String output;
	public static int k;
	public static Integer[] columns;
	public static int depth;
	
	public static int maxIteration = 5;
	public static double criterionValue = 0.1;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		input = new URI(args[0]).normalize().toString();
		output = new URI(args[1]).normalize().toString();
		k = Integer.parseInt(args[2]);
		depth = Integer.parseInt(args[3]);
		columns = Arrays.stream(Arrays.copyOfRange(args, 4, args.length))
				.map( s -> Integer.parseInt(s)).toArray(Integer[]::new);
		
		
		StartKMeans(input, output, columns);
	}
	
	public static void StartJob(String input, String output, String outputSufifx, int iteration, boolean lastIteration, Integer[] columns) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
	    
	    //Parameters
	    //number of clusters
	    conf.setInt("k", k);
	    //the number of the column to use from the CSV source file 
	    conf.setStrings("columns", Arrays.stream(columns)
				.map( s -> s.toString()).toArray(String[]::new));
	    //the number of iteration 
	    conf.setInt("iteration", iteration);
	    //the input path
	    conf.set("input", input);
	    //the output path
	    conf.set("output", output);
	    //is this iteration the final one 
	    conf.setBoolean("lastIteration", lastIteration);

	    Job job = Job.getInstance(conf, "kmeans");
	    job.setJarByClass(KDriver.class);
	    job.setMapperClass(KMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(KValue.class);
	    job.setReducerClass(KReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output+outputSufifx));
	    
	    for (int i=0; i<k; i++)
	    MultipleOutputs.addNamedOutput(job, i+"", TextOutputFormat.class, IntWritable.class, Text.class);
	    
	    job.waitForCompletion(true);
	}
	
	public static void StartKMeans(String input, String output, Integer[] columns) throws ClassNotFoundException, IOException, InterruptedException{
		
		boolean isDone = false;
		boolean lastIteration = false;
		int iteration = 1;
		
		KCentroidHelper.init(input, output, columns, k);
		
		while (!isDone) {
						
			if (lastIteration){
				StartJob(input, output, "/itFinal/", iteration, lastIteration, columns);
		    	isDone = true;
			}
		    else {
		    	StartJob(input, output, "/it"+iteration+"/", iteration, lastIteration, columns);
		    	lastIteration = (iteration >= maxIteration-1) ? true : KCentroidHelper.compareCentroids(iteration, output, criterionValue);
		    }
			iteration++;
		}
			
	}
	
	public static void StartHierarchicalKMeans(String input, String output, Integer[] columns, int currentDepth) throws ClassNotFoundException, IOException, InterruptedException{
		
		//TODO
			
	}

}
