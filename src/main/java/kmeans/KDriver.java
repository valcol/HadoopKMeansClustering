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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class KDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		int maxIteration = 50;
		double criterionValue = 0.1;
		
		boolean isDone = false;
		boolean lastIteration = false;
		
		int iteration = 1;
		String input = new URI(args[0]).normalize().toString();
		String output = new URI(args[1]).normalize().toString();
		int k = Integer.parseInt(args[2]);
		Integer[] columns = Arrays.stream(Arrays.copyOfRange(args, 3, args.length))
				.map( s -> Integer.parseInt(s)).toArray(Integer[]::new);
		
		KCentroidHelper.init(input, output, columns, k);
		
		while (!isDone) {
			
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
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+iteration+"/"));
			    job.waitForCompletion(true);
			    
			    if (lastIteration)
			    	isDone = true;
			    else
			    	lastIteration = (iteration == maxIteration-1) ? true : KCentroidHelper.compareCentroids(iteration, output, criterionValue);
			    
			    iteration++;
		}
	}

}
