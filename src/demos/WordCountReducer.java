package demos;

import java.util.Iterator;

import c2c.api.OutputCollector;
import c2c.api.Reducer;

public class WordCountReducer implements Reducer {

	@Override
	public void reduce(String key, Iterator<String> values,
			OutputCollector collector) {
		int count = 0;
		while (values.hasNext()) {
			count++;
			values.next();
		}
		collector.collect(key, String.valueOf(count));
	}

}
