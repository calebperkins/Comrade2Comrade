package c2c.api;

/**
 * Maps input key/value pairs to a set of intermediate key/value pairs.
 * 
 * A given input pair may map to zero or many output pairs.
 * 
 * For now, input and output key values are always Strings.
 * 
 * @author Caleb Perkins
 * 
 */
public interface Mapper {
	void map(String key, String value, OutputCollector collector);
}
