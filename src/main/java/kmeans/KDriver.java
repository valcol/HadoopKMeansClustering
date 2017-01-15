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

//hadoop jar target/tp3-mapreduce-0.0.1.jar /files/sample.csv /projet32 2 2 2 3
public class KDriver {
	
	public static String inputBase;
	public static String outputBase;
	public static int k;
	public static Integer[] columnsBase;
	public static int maxDepth;
	
	public static int maxIteration = 30;
	public static double criterionValue = 0.1;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		inputBase = new URI(args[0]).normalize().toString();
		outputBase = new URI(args[1]).normalize().toString();
		k = Integer.parseInt(args[2]);
		maxDepth = Integer.parseInt(args[3]);
		columnsBase = Arrays.stream(Arrays.copyOfRange(args, 4, args.length))
				.map( s -> Integer.parseInt(s)).toArray(Integer[]::new);
		
		StartHierarchicalKMeans(outputBase, 0);
	}
	
	public static void StartJob(String input, String output, String outputSufifx, int currentIteration, int currentDepth, boolean lastIteration) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
	    
	    //Parameters
	    //number of clusters
	    conf.setInt("k", k);
	    //the number of the column to use from the CSV source file 
	    conf.setStrings("columns", Arrays.stream(columnsBase)
				.map( s -> s.toString()).toArray(String[]::new));
	    //the number of iteration 
	    conf.setInt("currentIteration", currentIteration);
	    //the input path
	    conf.set("input", input);
	    //the output path
	    conf.set("output", output);
	    //is this iteration the final one 
	    conf.setBoolean("lastIteration", lastIteration);
	    //is the depth at max
	    conf.setBoolean("isMaxDepth", (currentDepth == maxDepth));
	    //the depth
	    conf.setInt("currentDepth", currentDepth);

	    Job job = Job.getInstance(conf, "kmeans");
	    job.setJarByClass(KDriver.class);
	    job.setMapperClass(KMapper.class);
	    job.setMapOutputKeyClass(KDoubleArrayWritable.class);
	    job.setMapOutputValueClass(KValueWritable.class);
	    job.setReducerClass(KReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    
	    //if (!maxDepth)
	    	FileOutputFormat.setOutputPath(job, new Path(output+outputSufifx));
	   // else
	    	//FileOutputFormat.setOutputPath(job, new Path(outputBase+"/results.csv"));
	    
	    for (int i=0; i<k; i++)
	    MultipleOutputs.addNamedOutput(job, i+"", TextOutputFormat.class, IntWritable.class, Text.class);
    
	    job.waitForCompletion(true);
	}
	
	public static void StartKMeans(String input, String output, int currentDepth) throws ClassNotFoundException, IOException, InterruptedException{
		
		boolean isDone = false;
		boolean lastIteration = false;
		int currentIteration = 0;
		
		while (!isDone) {
						
			if (lastIteration){
				StartJob(input, output, "/itFinal/", currentIteration, currentDepth, lastIteration);
		    	isDone = true;
			}
		    else {
		    	StartJob(input, output, "/it"+currentIteration+"/", currentIteration, currentDepth, lastIteration);
		    	if (currentIteration > 0)
		    		lastIteration = (currentIteration >= maxIteration-1) ? true : KCentroidHelper.compareCentroids(currentIteration, currentDepth, output, criterionValue);
		    }
			currentIteration++;
		}
			
	}
	
	public static void StartHierarchicalKMeans(String output, int currentDepth) throws ClassNotFoundException, IOException, InterruptedException{

		String customInput = (currentDepth > 0) ? outputBase+"/"+(currentDepth-1)+"/itFinal/part-r-00000" : inputBase;
		String customOutput = outputBase+"/"+currentDepth+"/";
		
		StartKMeans(customInput, customOutput, currentDepth);
		
		if (currentDepth < maxDepth)
			StartHierarchicalKMeans(customOutput, currentDepth+1);
			
	}

}
