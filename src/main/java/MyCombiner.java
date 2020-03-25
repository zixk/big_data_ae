

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Reducer;

public class MyCombiner extends Reducer<MyKey, Text, MyKey, Text> {
	/**
	 * Combiner aggregates output from the mappers depending on if the output is the
	 * term frequency of terms in articles or the document length
	 * 
	 */

	private Text new_value = new Text();

	@Override
	protected void reduce(MyKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if (key.tag.equals("TF")) {
			int sum = 0;
			for (Text value : values) {

				sum += Integer.parseInt(value.toString());

			}

			key.frequency = String.valueOf(sum);
			context.write(key, new Text());

			// output:<key,value>----<"term","docId:sum">
		}

		else {
			int sum = 0;
			for (Text value : values) {

				sum += Integer.parseInt(value.toString());

			}

			key.frequency = String.valueOf(sum);
			context.write(key, new Text());

		}
	}
}