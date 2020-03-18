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
		//int splitIndex = key.term.toString().indexOf(":");
		//String Tag=key.toString().substring(0,splitIndex);
		if(key.tag.equals("TF")) {
			int sum = 0;
			for(Text value : values){
				
				sum += Integer.parseInt(value.toString());
				
			}
			
			//int splitIndex2 = key.term.toString().indexOf(",");
			//new_value.set(key.term.toString().substring(splitIndex2+1)+":"+sum);
			//key.set(key.term.toString().substring(0,splitIndex2));
			//MyKey compositeKey= new MyKey(key.term.toString().substring(0,splitIndex2),new_value.toString());
			key.frequency= String.valueOf(sum);
			//context.write(key, new Text());
			output_files.write("Termfrequency",new Text(key.term+" "+key.docid), new Text(key.frequency));
			
	         //output:<key,value>----<"term","docId:sum">
		}
			
			else
			{
				int sum = 0;
				for(Text value : values){
					
					sum += Integer.parseInt(value.toString());
					
				}
				
				//MyKey compositeKey= new MyKey(key.term.toString(),Integer.toString(sum));
				key.frequency=String.valueOf(sum);
				output_files.write("Documentlength",new Text(key.docid), new Text(key.frequency));
				
			}
	    
		
		
		
		
			
             //output：<"MapReduce","0.txt:1,1.txt:1,2.txt:1">
}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		output_files.close();
	}
}
