package mapreduce.wcv3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
	private Text _value = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context
			context) throws IOException, InterruptedException {
		int splitIndex = key.toString().indexOf(":");
		String Tag=key.toString().substring(0,splitIndex);
	    
		
		if(Tag.equals("L")) {
		int sum = 0;
		for(Text value : values){
			
			sum += Integer.parseInt(value.toString());
		}
		this._value.set(Integer.toString(sum));
		context.write(key, this._value);
	}
		
		if(Tag.equals("TF")) {
			
			//input：<"TF:term",list("doc_id:1","doc_id:1","doc_id:1")>
			//output：<"TF:term","doc_id:1,doc_id:1,doc_id:1">
			String doc_list = new String();
			for(Text value : values){//value="doc_id:1"
				doc_list += value.toString()+";";
			}
			this._value.set(doc_list);
			context.write(key, this._value);
             //output：<"MapReduce","0.txt:1,1.txt:1,2.txt:1">
}
}
}