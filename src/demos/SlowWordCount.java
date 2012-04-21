package demos;

import c2c.api.Mapper;
import c2c.api.OutputCollector;
import c2c.api.Reducer;

/**
 * Simulates a slow mapping operation.
 * @author Caleb Perkins
 *
 */
public class SlowWordCount implements Reducer, Mapper {

	@Override
	public void map(String key, String value, OutputCollector collector) {
		String[] words = value.split("\\s+");
		for (String w : words) {
			long t0, t1; // busy-wait for 3 seconds
			t0 = System.currentTimeMillis();
			do {
				t1 = System.currentTimeMillis();
			} while ((t1 - t0) < 3 * 1000);
			collector.collect(w, "1");
		}
	}

	@Override
	public void reduce(String key, Iterable<String> values,
			OutputCollector collector) {
		int count = 0;
		for (String s : values) {
			count += Integer.parseInt(s);
		}
		collector.collect(key, String.valueOf(count));
	}

}
