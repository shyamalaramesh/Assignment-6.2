package mapreduce.task9;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task9Mapper extends Mapper<Text, IntWritable, IntWritable, Text>{
	
	/**
	 * The output from the previous task is a SequenceFileInputFormat. There the output key 
	 * type was text (company name) and the output value type was IntWritable (number of units sold). 
	 * That state is stored directly in the sequence file.  
	 * So the input key type in this job is Text (company name) and the input value type is IntWritable 
	 *   
	 * We want to sort the data based on the total units sold. Hence the 
	 * output key is total units sold and the value is the company name. So, output 
	 * key type is IntWritable and output value type is Text
	 * 
	 */
	public void map(Text companyName, IntWritable totalUnitsSold, Context context) 
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int minTotalUnitsSold = conf.getInt(Task9Constants.MIN_TOTAL_UNITS_SOLD_KEY, Task9Constants.MIN_DEFAULT_TOTAL_UNITS_SOLD);
		int maxTotalUnitsSold = conf.getInt(Task9Constants.MAX_TOTAL_UNITS_SOLD_KEY, Task9Constants.MAX_DEFAULT_TOTAL_UNITS_SOLD);
		int totUnitsSold = totalUnitsSold.get();
		if(totUnitsSold >= minTotalUnitsSold && totUnitsSold <= maxTotalUnitsSold) {
			context.write(totalUnitsSold, companyName);
		}
	} 
}
