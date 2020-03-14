package mapreduce.wcv3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Reducer;
public  class CustomCombiner extends Reducer<Text, Text, Text, Text>{

	private Text new_value = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		//input：<key,value>---<"term:docId",list(1,1,1,1)>
		//key="term:docId",value=list(1,1,1,1);
		int splitIndex = key.toString().indexOf(":");
		String Tag=key.toString().substring(0,splitIndex);
		if(Tag=="TF") {
		int sum = 0;
		for(Text value : values){
			
			sum += Integer.parseInt(value.toString());
		}
		
		int splitIndex2 = key.toString().indexOf(",");
		new_value.set(key.toString().substring(splitIndex2+1)+":"+sum);
		key.set(key.toString().substring(0,splitIndex2));
		context.write(key, new_value);
         //output:<key,value>----<"term","docId:sum">
	}	
}
}