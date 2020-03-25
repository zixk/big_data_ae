package mapreduce.wcv3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.URI;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import utils.PorterStemmer;

public class MyMapper extends Mapper<LongWritable, Text, MyKey, Text> {
	/**
	 * Receives an input split and performs tokenisation, stopword removal and
	 * stemming Outputs each term and their respective frequencies
	 * 
	 * accepts LongWritable, Text, and outputs MyKey, Text
	 */

	private Text _key = new Text();
	private Text _value = new Text();// first k-value pair

	private Text _key2 = new Text();
	private Text _value2 = new Text();// the second k-value pair

	private final static Logger log = Logger.getLogger("myMapper");
	private List<String> stopwords = new ArrayList<String>(); // to load all stopwords

	private PorterStemmer stemmer = new PorterStemmer(); // stemming the words

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		URI[] cacheFiles = context.getCacheFiles();
		if (cacheFiles != null && cacheFiles.length > 0) {
			try {
				List<String> stopwords = new ArrayList<String>();
				BufferedReader reader = new BufferedReader(new FileReader("stopwords"));
				String word = null;
				while ((word = reader.readLine()) != null) {
					log.info("WORD - " + word);
					stopwords.add(word.trim());
				}
				reader.close();
			} finally {

			}
		}
	}

	public boolean CheckStopwords(String word) throws IOException {

		return stopwords.contains(word);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// to store each term in the document with its frequency
		HashMap<String, Integer> Term_frequency = new HashMap<String, Integer>();

		// loadStopwords();
		String valueAsString = value.toString().toLowerCase();
		valueAsString = valueAsString.replaceAll("[^a-zA-Z0-9\\s]", " ");
		valueAsString = valueAsString.replaceAll("\n", " ");
		valueAsString = removeStopwords(valueAsString);
		// tokenize inputsplit by a space delimiter
		StringTokenizer tokenizer = new StringTokenizer(valueAsString.toString(), " ");

		while (tokenizer.hasMoreTokens()) {

			String word = tokenizer.nextToken(); // grab the first token

			int sep = word.indexOf(' ');
			if (!CheckStopwords(word)) {
				word = (sep == -1) ? word : word.substring(0, word.indexOf(' '));

				if (!Term_frequency.containsKey(word)) // adding the word with frequency 1 if it is not in the hashmap
					Term_frequency.put(word, 1);
				else {
					// if it is already in the hashmap, retrieve the value, increment by 1, store
					// the new frequency
					int new_value = Term_frequency.get(word) + 1;
					Term_frequency.replace(word, new_value);
				}

			}
		}

		for (String term : Term_frequency.keySet()) {
			// emitting each term with its frequency
			MyKey compositeKey1 = new MyKey("TF", term, key.toString(), Term_frequency.get(term).toString());
			context.write(compositeKey1, new Text()); // e.g. (TF,book,docid,25,null)
		}

		// emitting docid with its length which is equal to the size of the hashmap
		MyKey compositeKey2 = new MyKey("L", key.toString(), key.toString(), Integer.toString(Term_frequency.size()));
		context.write(compositeKey2, new Text());
		System.out.println("mapper");
		for (String i : Term_frequency.keySet()) {
			System.out.println("Key: " + i + " value: " + Term_frequency.get(i) + " docid :" + key);
		}

	}

	private void loadStopwords() throws IOException {
		// reads in the stopwords into a list
		stopwords = Files
				.readAllLines(Paths.get("hdfs://bigdata-10.dcs.gla.ac.uk:8020/user/2144751b/stopword-list.txt"));
	}

	private String removeStopwords(String original) {
		String[] allWords = original.split(" ");

		StringBuilder builder = new StringBuilder();
		for (String word : allWords) {
			if (!stopwords.contains(word)) {
				word = stemmer.stem(word); // stems the word
				builder.append(word);
				builder.append(' ');
			}
		}

		return builder.toString().trim();
	}
}
