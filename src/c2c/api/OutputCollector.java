package c2c.api;

/**
 * Collects the <key, value> pairs output by Mappers and Reducers.
 * 
 * OutputCollector is the generalization of the facility provided by the
 * Map-Reduce framework to collect data output by either the Mapper or the
 * Reducer i.e. intermediate outputs or the output of the job.
 */
public interface OutputCollector {
	void collect(String key, String value);
}
