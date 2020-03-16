package mapreduce.wcv3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Reducer;
public  class MyCombiner extends Reducer<MyKey, Text, MyKey, Text>{

	private Text new_value = new Text();
	@Override
	protected void reduce(MyKey key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		//input：<key,value>---<"term:docId",list(1,1,1,1)>
		//key="term:docId",value=list(1,1,1,1);
		int splitIndex = key.term.toString().indexOf(":");
		String Tag=key.toString().substring(0,splitIndex);
		if(Tag.equals("TF")) {
		int sum = 0;
		for(Text value : values){
			
			sum += Integer.parseInt(value.toString());
		}
		
		int splitIndex2 = key.term.toString().indexOf(",");
		new_value.set(key.term.toString().substring(splitIndex2+1)+":"+sum);
		//key.set(key.term.toString().substring(0,splitIndex2));
		MyKey compositeKey= new MyKey(key.term.toString().substring(0,splitIndex2),new_value.toString());
		context.write(compositeKey, new Text());
         //output:<key,value>----<"term","docId:sum">
	}
		
		else
		{
			int sum = 0;
			for(Text value : values){
				
				sum += Integer.parseInt(value.toString());
			}
			
			MyKey compositeKey= new MyKey(key.term.toString(),Integer.toString(sum));
			context.write(compositeKey, new Text());
		}
}
}