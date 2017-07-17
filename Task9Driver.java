package mapreduce.task9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Task9Driver {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// initialize the configuration
		Configuration conf = new Configuration();
		
		/*
		 * The minTotalUntisSold and maxTotalUnitsSold properties are stored in Configuration object. This object 
		 * can later be accessed in the mapper to filter out records and in the partitioner to find out the partition 
		 * to which a key has to go to 
		 */
		conf.setInt(Task9Constants.MIN_TOTAL_UNITS_SOLD_KEY, Integer.parseInt(args[3]));
		conf.setInt(Task9Constants.MAX_TOTAL_UNITS_SOLD_KEY, Integer.parseInt(args[4]));
		
		// create a job object from the configuration and give it any name you want 
		Job job = Job.getInstance(conf, "Assignment_6.2 -> Task_9 -> "
									+ "Sort_Number_Of_Units_Sold_Multiple_Reducers");
		
		// java.lang.Class object of the driver class
		job.setJarByClass(Task9Driver.class);

		// map function outputs key-value pairs. 
		// What is the type of the key in the output 
		// We want to sort based on the total units sold
		// The reduce phase does sorting based on the key
		// Hence the key type output by the mapper will be IntWritable 
		job.setMapOutputKeyClass(IntWritable.class);
		// map function outputs key-value pairs. 
		// What is the type of the value in the output
		// In this case the value output by the mapper 
		// will be the company name. So the type is Text
		job.setMapOutputValueClass(Text.class);
		
		// reduce function outputs key-value pairs. 
		// What is the type of the key in the output. 
		// In this case output is number of units sold per company. 
		// Key will be company name, So the type is Text
		job.setOutputKeyClass(Text.class);
		// reduce function outputs key-value pairs. 
		// What is the type of the value in the output. 
		// In this case output is number of units sold by the company 
		// So value type is IntWritable 
		job.setOutputValueClass(IntWritable.class);
		
		// Mapper class which has implemenation for the map phase
		job.setMapperClass(Task9Mapper.class);
		
		/* For this implementation we need a custom partitioner because 
		 * there are multiple reducers and the requirement is to sort based on 
		 * the number of units sold. If two key value pairs with different keys go 
		 * to different reducers in a random order, then the final aggregated output 
		 * from all the reducers will not be sorted. So we need to ensure that the 
		 * lowest n keys need to go to the 1st reducer and the next lowest n keys 
		 * to go to the 2nd reducer and so on. The highest n keys will have to go to 
		 * the last reducer  
		 * 
		 */
		job.setPartitionerClass(Task9Partitioner.class);
		
		// Reducer class which has implementation for the reduce phase
		job.setReducerClass(Task9Reducer.class);
		
		//set the number of reduce tasks
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		// Input is a sequence file output by task 8_1. So input format class is SequenceFileInputFormat
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		/* 
		 * Output file is a simple text file. So, output format is TextOutputFormat
		 */
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * The input path to the job. The map task will
		 * read the files in this path form HDFS 
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		/*
		 * The output path from the job. The map/reduce task will
		 * write the files to this path to HDFS. In this case the 
		 * reduce task will write to output path because number of 
		 * reducer tasks is not explicitly configured to be zero  
		 */
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);

	}
}
