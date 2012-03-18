package c2c.api;

import java.util.Iterator;

/**
 * Reduces a set of intermediate values which share a key to a smaller set of values.
 * @author Cakeb Perkins
 *
 */
public interface Reducer {
	void reduce(String key, Iterator<String> values, OutputCollector collector);
}
