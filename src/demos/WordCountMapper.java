package demos;

import c2c.api.Mapper;
import c2c.api.OutputCollector;

public class WordCountMapper implements Mapper {

	@Override
	public void map(String key, String value, OutputCollector collector) {
		String[] words = value.split("\\s+");
		for (String w : words) {
			collector.collect(w, "1");
		}
	}

}
