package demos;

import c2c.api.MapReduceApplication;
import c2c.api.OutputCollector;

/**
 * Simulates a slow mapping and reducing operation.
 * @author Caleb Perkins
 *
 */
public class SlowWordCount implements MapReduceApplication {
	private static final int WAIT_SECONDS = 20;

	@Override
	public void map(String key, String value, OutputCollector collector) {
		long t0, t1; // busy-wait for 20 seconds
		t0 = System.currentTimeMillis();
		do {
			t1 = System.currentTimeMillis();
		} while ((t1 - t0) < WAIT_SECONDS * 1000);
		
		String[] words = value.split("\\s+");
		for (String w : words) {
			collector.collect(w, "1");
		}
	}

	@Override
	public void reduce(String key, Iterable<String> values,
			OutputCollector collector) {
		long t0, t1; // busy-wait for 20 seconds
		t0 = System.currentTimeMillis();
		do {
			t1 = System.currentTimeMillis();
		} while ((t1 - t0) < WAIT_SECONDS * 1000);
		
		int count = 0;
		for (String s : values) {
			count += Integer.parseInt(s);
		}
		collector.collect(key, String.valueOf(count));
	}

}
