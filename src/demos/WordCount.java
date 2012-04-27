package demos;

import c2c.api.MapReduceApplication;
import c2c.api.OutputCollector;

public class WordCount implements MapReduceApplication {

	@Override
	public void map(String key, String value, OutputCollector collector) {
		String[] words = value.split("\\s+");
		for (String w : words) {
			collector.collect(w, "1");
		}
	}

	@Override
	public void reduce(String key, Iterable<String> values,
			OutputCollector collector) {
		int count = 0;
		for (String s : values) {
			count += Integer.parseInt(s);
		}
		collector.collect(key, String.valueOf(count));
	}

}
