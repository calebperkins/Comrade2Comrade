package demos;

import java.util.TreeSet;

import c2c.api.MapReduceApplication;
import c2c.api.OutputCollector;

public class InvertedIndex implements MapReduceApplication {

	/**
	 * The map function parses each document, and emits a sequence of <word, document ID> pairs. 
	 */
	@Override
	public void map(String key, String value, OutputCollector collector) {
		String[] words = value.split("[^A-Za-z]");
		for (String word : words) {
			collector.collect(word.toLowerCase(), key);
		}
	}

	/**
	 * The reduce function accepts all pairs for a given word, sorts the corresponding document IDs
	 * and emits a <word, list(document ID)> pair.
	 */
	@Override
	public void reduce(String key, Iterable<String> values,
			OutputCollector collector) {
		TreeSet<String> sorted = new TreeSet<String>();
		for (String v : values) {
			sorted.add(v);
		}
		StringBuilder string = new StringBuilder();
		string.append("[");
		for (String v : sorted) {
			string.append(v);
			string.append(", ");
		}
		string.append("]");
		collector.collect(key, string.toString());
	}

}
