package mapreduce.task9;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task9Reducer extends Reducer<IntWritable, Text, Text, IntWritable>{
	
	/**
	 * The mapper outputs total units sold for a company as key and company name as value.
	 * The partitioner is written such that the lowest n numbers go to the first reducer. The next lowest 
	 * n numbers go to the 2nd reducer and so on. So, in totality, the input records are already sorted 
	 * when they reach the reduce method 
	 * 
	 * In the input, the company name was followed by the total number of units osld in each line. 
	 * To maintain the same, the reducer outputs the company name as the key and total units sold as the value.
	 * So, the input key type is Text and the output value type is IntWritable  
	 */
	@Override
	protected void reduce(IntWritable totalUnitsSold, Iterable<Text> companies,
			Reducer<IntWritable, Text, Text, IntWritable>.Context context) 
					throws IOException, InterruptedException {
		
		for(Text companyName : companies) {
			context.write(companyName, totalUnitsSold);
		}
		
	}
	
}
