package c2c.api;

import java.util.Iterator;

public interface Reducer {
	void reduce(String key, Iterator<String> values, OutputCollector collector);
}
