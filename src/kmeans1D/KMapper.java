package kmeans1D;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	 public int k = 0;
	 public int column = 0;
	 
	  @Override
	  public void setup(Context context){
		  
		  //Get job configuration
		  Configuration conf = context.getConfiguration();
		  column = (int) conf.getInt("column", 10);
		  k = (int) conf.getInt("k", 10);
	  }
	  

	@Override
	public void map(LongWritable key, Text value,
			 Context context) throws IOException {

		

	}

}