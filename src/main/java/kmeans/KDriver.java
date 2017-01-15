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
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
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
	private static int mesureCol;
	private static int labelCol;
	
	public static int maxIteration = 2;
	public static double criterionValue = 0.1;


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		inputBase = new URI(args[0]).normalize().toString();
		outputBase = new URI(args[1]).normalize().toString();
		k = Integer.parseInt(args[2]);
		maxDepth = Integer.parseInt(args[3]);
		labelCol = Integer.parseInt(args[4]);
		mesureCol = Integer.parseInt(args[5]);
		columnsBase = Arrays.stream(Arrays.copyOfRange(args, 6, args.length))
				.map( s -> Integer.parseInt(s)).toArray(Integer[]::new);
		
		StartHierarchicalKMeans(outputBase, 0);
	}
	
	public static void StartJob(String customInput, String customOutput, String outputSufifx, int currentIteration, int currentDepth, boolean lastIteration) throws IOException, ClassNotFoundException, InterruptedException{
		
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
	    conf.set("input", customInput);
	    //the output path
	    conf.set("output", customOutput);
	    //is this iteration the final one 
	    conf.setBoolean("lastIteration", lastIteration);
	    //is the depth at max
	    conf.setBoolean("isMaxDepth", (currentDepth == maxDepth));
	    //the depth
	    conf.setInt("currentDepth", currentDepth);
	    //the label column
	    conf.setInt("labelCol", labelCol);
	    //the mesure column
	    conf.setInt("mesureCol", mesureCol);

	    Job job = Job.getInstance(conf, "kmeans");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(KDriver.class);
	    job.setMapperClass(KMapper.class);
	    job.setMapOutputKeyClass(KDoubleArrayWritable.class);
	    job.setMapOutputValueClass(KValueWritable.class);
	    job.setReducerClass(KReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(customInput));

	    FileOutputFormat.setOutputPath(job, new Path(customOutput+outputSufifx));

	    MultipleOutputs.addNamedOutput(job, "data", TextOutputFormat.class, IntWritable.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "labels", TextOutputFormat.class, IntWritable.class, Text.class);

    
	    job.waitForCompletion(true);
	}
	
	public static void StartKMeans(String customInput, String customOutput, int currentDepth) throws ClassNotFoundException, IOException, InterruptedException{
		
		boolean isDone = false;
		boolean lastIteration = false;
		int currentIteration = 0;
		
		while (!isDone) {
						
			if (lastIteration){
				StartJob(customInput, customOutput, "/itFinal/", currentIteration, currentDepth, lastIteration);
		    	isDone = true;
			}
		    else {
		    	StartJob(customInput, customOutput, "/it"+currentIteration+"/", currentIteration, currentDepth, lastIteration);
		    	if (currentIteration > 0)
		    		lastIteration = (currentIteration >= maxIteration-1) ? true : KCentroidHelper.compareCentroids(currentIteration, currentDepth, customOutput, criterionValue);
		    }
			currentIteration++;
		}
			
	}
	
	public static void StartHierarchicalKMeans(String output, int currentDepth) throws ClassNotFoundException, IOException, InterruptedException{

		String customInput = (currentDepth > 0) ? outputBase+"/temp/"+(currentDepth-1)+"/itFinal/data-r-00000" : inputBase;
		String customOutput = outputBase+"/temp/"+currentDepth+"/";
		
		StartKMeans(customInput, customOutput, currentDepth);
		
		if (currentDepth < maxDepth)
			StartHierarchicalKMeans(customOutput, currentDepth+1);
		else 
			CleanUp();
			
	}

	private static void CleanUp() {

		//TODO
		
	}

}
