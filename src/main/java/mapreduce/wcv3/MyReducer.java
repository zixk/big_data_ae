package mapreduce.wcv3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReducer extends Reducer<MyKey, Text, Text, Text> {
	private Text _value = new Text();
	 private MultipleOutputs output_files;
	 
	@Override 
	public void setup(Context context) {
		output_files=new MultipleOutputs(context);
	}
	 
	@Override
	protected void reduce(MyKey key, Iterable<Text> values, Context
			context) throws IOException, InterruptedException {
		
		if(key.tag.equals("TF")) {
			for(Text v:values)
			{
			System.out.println("key:"+key.term+" : docid :"+key.docid+ " : "+"termfre: "+key.frequency);
			output_files.write("Termfrequency",new Text(key.term+" : "+key.docid+ " : "), new Text(key.frequency));
			
	         //output:term : docid : frequency 
		}
		}
			
			else
			{
				
				output_files.write("Documentlength",new Text(key.docid +" : "), new Text(key.frequency));
				 //output:docid : length
				
			}
	    
}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		output_files.close();
	}
}
