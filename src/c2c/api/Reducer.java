package c2c.api;

/**
 * Reduces a set of intermediate values which share a key to a smaller set of
 * values.
 * 
 * @author Cakeb Perkins
 * 
 */
public interface Reducer {
	void reduce(String key, Iterable<String> values, OutputCollector collector);
}
