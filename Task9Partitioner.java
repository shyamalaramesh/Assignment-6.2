package mapreduce.task9;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task9Partitioner extends Partitioner<IntWritable, Text> implements Configurable{

	private Configuration conf;
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int getPartition(IntWritable totalUnitsSold, Text companyName, int numReduceTasks) {
		
		// total units sold as the key output by the mapper
		int totUnitsSold = totalUnitsSold.get();
		
		// obtaining the minTotalUnitsSoldInput from the configuration
		int minTotalUnitsSoldInput = conf.getInt(Task9Constants.MIN_TOTAL_UNITS_SOLD_KEY, Task9Constants.MIN_DEFAULT_TOTAL_UNITS_SOLD);
		
		// obtaining the maxTotalUnitsSoldInput from the configuration2
		int maxUnitsSoldInput = conf.getInt(Task9Constants.MAX_TOTAL_UNITS_SOLD_KEY,Task9Constants.MAX_DEFAULT_TOTAL_UNITS_SOLD);
		
		/* assuming the totalUnitsSold is uniformly distributed, calculating the size of each partition
		 * (the number of keys that will go to a particular reducer)
		*/
		int partitionSize = (maxUnitsSoldInput - minTotalUnitsSoldInput)/numReduceTasks;
		
		/*
		 * Calculating the partition number that this key value will fall in
		 */
		int partitionNumber = (totUnitsSold - minTotalUnitsSoldInput) / partitionSize;
		
		/* Safety check. If the partition number calculated above happens to be negative,
		 * route it to the first reducer. If the partition number happens to be not less than 
		 * the number of reducers assign it to the last reducer. This should ideally not happen 
		 * because the  map phase already filters out those records which are not within the min and max  
		 */
		
		if(partitionNumber < 0 ) {
			partitionNumber = 0;
		} else if(partitionNumber >= numReduceTasks) {
			partitionNumber = numReduceTasks - 1;
		}
		return partitionNumber;
	}

}
