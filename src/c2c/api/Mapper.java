package c2c.api;

/**
 * Maps one domain of key-values to another. For now, input and output key values are always Strings.
 * @author Caleb Perkins
 *
 */
public interface Mapper {
	void map(String key, String value, OutputCollector collector);
}
