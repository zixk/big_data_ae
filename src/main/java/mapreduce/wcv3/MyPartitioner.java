package mapreduce.wcv3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyKey, Text> {
	/**
	 * Partitions/groups output from the mappers into groups based on a hashing
	 * function. Groups are sent to reducers
	 */
	@Override
	public int getPartition(MyKey key, Text value, int numPartitions) {

		return (key.term.hashCode() & Integer.MAX_VALUE) % numPartitions;

	}
}
