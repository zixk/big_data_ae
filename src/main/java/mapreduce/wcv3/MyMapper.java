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

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {// accepts LongWritable, Text, and outputs Text, Text
	static enum Counters { NUM_RECORDS, NUM_LINES, NUM_BYTES }
	private Text _key = new Text();
	private Text _value = new Text();
	
	private Text _key2 = new Text();
	private Text _value2 = new Text();// the second k-value pair
	
	private static List<String> stopwords;
	
	
   private PorterStemmer stemmer = new PorterStemmer();
   public static boolean CheckStopwords(String word) throws IOException {
			
		return stopwords.contains(word);
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException {
		loadStopwords();
		StringTokenizer tokenizer = new StringTokenizer(value.toString(), " "); // tokenize inputsplit by a space delimeter
		
		while (tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken().toLowerCase(); // grab the first token
			//line=stemmer.stem(line).toLowerCase(); // lowercase all text in inputsplit
			word = word.replaceAll("[^a-zA-Z0-9\\s]", ""); // removes all special characters and punctuation marks
			//line = removeStopwords(line); // stopword removal and stemming
			//System.out.println(line);
			int sep = word.indexOf(' ');
			if(!CheckStopwords(word)){
			this._key.set((sep == -1) ? "TF:"+word+","+ key.toString(): "TF:"+word.substring(0, word.indexOf(' '))+","+ key.toString());// a tag is added to the key to differentiate between 2 key-value pairs 
			
			this._value.set("1");
			
			context.write(this._key, this._value);// emit first k-value pair ({TF:term,doc_id}, 1)
			this._key2.set("L:"+key.toString().toString()); // document id with L tag to differentiate between 2 k-value pairs
			this._value2.set("1");// 
			
			context.write(this._key2, this._value2);// emit the second k-value pair ({L:doc_id},1)// i did this so the combiner can deal with all situation
			
			//context.getCounter(Counters.NUM_LINES).increment(1);
			}
		}
		
		//context.getCounter(Counters.NUM_BYTES).increment(value.getLength());
		//context.getCounter(Counters.NUM_RECORDS).increment(1);
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
