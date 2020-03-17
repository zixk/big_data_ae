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
		
	    
		
		if(key.tag.equals("L")) {
		
		
		output_files.write("Documentlength",new Text(key.docid), new Text(key.frequency));
	}
		
		if(key.tag.equals("TF")) {
			
			//input：<"TF:term",list("doc_id:1","doc_id:1","doc_id:1")>
			//output：<"TF:term","doc_id:1,doc_id:1,doc_id:1">
			//String doc_list = new String();
			//for(Text value : values){//value="doc_id:1"
			//	doc_list += value.toString()+";";
			//this._value.set(doc_list);
			output_files.write("Termfrequency",new Text(key.term), new Text(key.frequency));
			}
			
             //output：<"MapReduce","0.txt:1,1.txt:1,2.txt:1">
}
}
