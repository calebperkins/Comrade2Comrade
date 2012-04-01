package demos;

import c2c.api.*;
import c2c.application.*;
import c2c.utilities.Constants;
import c2c.utilities.Constants.WORK_TYPE;

public class WordCount implements Task<OutputCollector> {
	Constants.WORK_TYPE work_type = null;
	OutputCollector collector;
	
	String key, value;
	Iterable<String> values;
	
	public WordCount(OutputCollector collector) {
		this.collector = collector;
	}
	
	public void prepareToMap(String key, String value) {
		work_type = WORK_TYPE.MAP;
		this.key = key;
		this.value = value;
	}
	
	public void prepareToReduce(String key, Iterable<String> values) {
		work_type = WORK_TYPE.REDUCE;
		this.key = key;
		this.values = values;
	}
	
	private void map(String key, String value, OutputCollector collector) {
		String[] words = value.split("\\s+");
		for (String w : words) {
			collector.collect(w, "1");
		}
	}

	private void reduce(String key, Iterable<String> values,
			OutputCollector collector) {
		int count = 0;
		for (String s : values) {
			count++;
		}
		collector.collect(key, String.valueOf(count));
	}

	@Override
	public OutputCollector execute() throws UnsupportedOperationException {
		if (work_type == WORK_TYPE.MAP) {
			map(key, value, collector);
			return collector;
		} else if (work_type == WORK_TYPE.REDUCE) {
			reduce(key, values, collector);
			return collector;
		} else {
			throw new UnsupportedOperationException("Haven't initialized arguments yet");
		}
	}

}
