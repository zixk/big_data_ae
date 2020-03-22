package mapreduce.wcv3;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration myconf = getConf();

		// The following two lines instruct Hadoop/MapReduce to run in local
		// mode. In this mode, mappers/reducers are spawned as thread on the
		// local machine, and all URLs are mapped to files in the local disk.
		// Remove these lines when executing your code against the cluster.
		//myconf.set("mapreduce.framework.name", "local");
        //myconf.set("fs.defaultFS", "file:///");
        myconf.set("textinputformat.record.delimiter", "\n[[");
		Job job = Job.getInstance(myconf);
		FileSystem fs = FileSystem.get(myconf);
		fs.copyFromLocalFile(new Path("file:///users/level5/2144751b/big_data_ae/src/main/resources/stopword-list.txt"), 
				new Path("hdfs://bigdata-10.dcs.gla.ac.uk:8020/user/2144751b/stopword-list.txt"));
		job.addCacheFile(new URI("hdfs://bigdata-10.dcs.gla.ac.uk:8020/user/2144751b/stopword-list.txt" + "#stopwords"));
		job.setJobName("Indexer");
		job.setJarByClass(WordCount.class);
		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		//job.setCombinerClass(MyCombiner.class);
		job.setSortComparatorClass(MYSortComparator.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(job.getJobName() +"_output"));
		MultipleOutputs.addNamedOutput(job, "Termfrequency", TextOutputFormat.class, Text.class, Text.class);
	    MultipleOutputs.addNamedOutput(job, "Documentlength", TextOutputFormat.class, Text.class, Text.class);
		int i= job.waitForCompletion(true) ? 0 : 1;
		if(i==0)
			
			System.out.print(job.getCounters());
		return i;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCount(), args));
	}
}