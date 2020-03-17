package mapreduce.wcv3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyKey, Text> {
	@Override
	public int getPartition(MyKey key, Text value, int numPartitions) {
		
		//int splitIndex = key.term.toString().indexOf(":");
		//String Tag=key.term.toString().substring(0,splitIndex);
		if(key.tag.equals("TF")) {
		int c = Character.toLowerCase(key.term.toString().charAt(3));
		if (c < 'a' || c > 'z')
			return numPartitions - 1;
		return (int)Math.floor((float)(numPartitions - 2) * (c-'a')/('z'-'a'));
	}
		
		else
		{
			return(key.term.hashCode() & Integer.MAX_VALUE)%numPartitions;// default
		}
}
}
