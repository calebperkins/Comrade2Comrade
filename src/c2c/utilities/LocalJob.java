package c2c.utilities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import c2c.payloads.KeyValue;

/**
 * A job that this node is acting as a master for.
 * @author Caleb Perkins
 *
 */
public class LocalJob {
	private final Set<String> completed;
	private int expected;
	
	public LocalJob(Set<KeyValue> input) {
		expected = input.size();
		completed = new HashSet<String>();
	}
	
	public void nowReducing(int expected_reducers) {
		expected = expected_reducers;
		completed.clear();
	}
	
	public void reductionDoneFor(String key_data) {
		completed.add(key_data);
	}
	
	public void mappingDoneFor(String key_data) {
		completed.add(key_data);
	}
	
	public boolean reducingComplete() {
		return completed.size() == expected;
	}
	
	public boolean mappingComplete() {
		return completed.size() == expected;
	}
	
	private static Map<String, LocalJob> jobs = new HashMap<String, LocalJob>();
	
	public static LocalJob get(String domain) {
		return jobs.get(domain);
	}
	
	public static void put(String domain, LocalJob job) {
		jobs.put(domain, job);
	}
}
