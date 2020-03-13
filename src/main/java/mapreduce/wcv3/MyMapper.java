package mapreduce.wcv3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
	private static List<String> stopwords;
	private PorterStemmer stemmer = new PorterStemmer();
	
	public static boolean CheckStopwords(String word) throws IOException {
		
		System.out.println();
				
		return stopwords.contains(word);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException {
		loadStopwords();
		StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n"); // tokenize inputsplit by a newline delimeter
		
		while (tokenizer.hasMoreTokens()) {
			String line = tokenizer.nextToken().toLowerCase(); // grab the first token
			//line=stemmer.stem(line).toLowerCase(); // lowercase all text in inputsplit
			line = line.replaceAll("[^a-zA-Z0-9\\s]", ""); // removes all special characters and punctuation marks
			line = removeStopwords(line); // stopword removal and stemming	
			int sep = line.indexOf(' ');
			if(!CheckStopwords(line)){
			this._key.set((sep == -1) ? line: line.substring(0, line.indexOf(' ')));
			Text outValue=new Text();
			outValue.set(key.toString()+","+"1");
			//this._value.set(outValue);// how can we pass String value (documentID, tf ) to the mapper
			//context.write(this._key, this._value);
			context.getCounter(Counters.NUM_LINES).increment(1);
			}
		}
		context.getCounter(Counters.NUM_BYTES).increment(value.getLength());
		context.getCounter(Counters.NUM_RECORDS).increment(1);
	}
	
	private void loadStopwords() throws IOException{
		String stopwordsFileLocation = getClass().getClassLoader().getResource("stopword-list.txt").getPath(); // finds the path to the stopwords file in the resources folder
		stopwords = Files.readAllLines(Paths.get(stopwordsFileLocation)); // reads in the stopwords into a list 
	}
	
	private String removeStopwords(String original) {
		String[] allWords = original.split(" ");
		
		StringBuilder builder = new StringBuilder();
		for(String word: allWords) {
			if(!stopwords.contains(word)) {
				word = stemmer.stem(word); // stems the word
				builder.append(word);
				builder.append(' ');
			}
		}
		
		return builder.toString().trim();
	}
}
