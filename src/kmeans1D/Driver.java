package kmeans1D;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
			    Configuration conf = new Configuration();
			    
			    //Parameters
			    //number of clusters
			    conf.setInt("k", Integer.parseInt(args[2]));
			    //the number of the column to use from the CSV source file 
			    conf.setInt("column", Integer.parseInt(args[3]));

			    Job job = Job.getInstance(conf, "kmeans1D");
			    job.setNumReduceTasks(1);
			    job.setJarByClass(Driver.class);
			    job.setMapperClass(KMapper.class);
			    job.setMapOutputKeyClass(IntWritable.class);
			    job.setMapOutputValueClass(Text.class);
			    job.setCombinerClass(KReducer.class);
			    job.setReducerClass(KReducer.class);
			    job.setOutputKeyClass(IntWritable.class);
			    job.setOutputValueClass(Text.class);
			    job.setOutputFormatClass(TextOutputFormat.class);
			    job.setInputFormatClass(TextInputFormat.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
