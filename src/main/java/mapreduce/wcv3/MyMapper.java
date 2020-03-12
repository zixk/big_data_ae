package mapreduce.wcv3;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.PorterStemmer;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	static enum Counters { NUM_RECORDS, NUM_LINES, NUM_BYTES }
	private Text _key = new Text();
	private IntWritable _value = new IntWritable();
	
	
	public static boolean CheckStopwords(String word) throws IOException {
		List<String> stopwords=Arrays.asList("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "you're", "you've", "you'll", "you'd", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "she's", "her", "hers", "herself", "it", "it's", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "that'll", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "don't", "should", "should've", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren", "aren't", "couldn", "couldn't", "didn", "didn't", "doesn", "doesn't", "hadn", "hadn't", "hasn", "hasn't", "haven", "haven't", "isn", "isn't", "ma", "mightn", "mightn't", "mustn", "mustn't", "needn", "needn't", "shan", "shan't", "shouldn", "shouldn't", "wasn", "wasn't", "weren", "weren't", "won", "won't", "wouldn", "wouldn't");
				
		return stopwords.contains(word);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
		PorterStemmer stemmer = new PorterStemmer();
		while (tokenizer.hasMoreTokens()) {
			String line = tokenizer.nextToken();
			line=stemmer.stem(line).toLowerCase();
			int sep = line.indexOf(' ');
			if(!CheckStopwords(line)){
			this._key.set((sep == -1) ? line: line.substring(0, line.indexOf(' ')));
			Text outValue=new Text();
			outValue.set(key.toString()+","+"1");
			this._value.set(outValue);// how can we pass String value (documentID, tf ) to the mapper
			context.write(this._key, this._value);
			context.getCounter(Counters.NUM_LINES).increment(1);
			}
		}
		context.getCounter(Counters.NUM_BYTES).increment(value.getLength());
		context.getCounter(Counters.NUM_RECORDS).increment(1);
	}
}
