package c2c.utilities;

import java.util.*;
import org.joda.time.*;

public class WorkerTable {
	// Number of seconds to wait before a missing job is killed
	protected static final Duration TIMEOUT = new Duration(10 * 1000);
	protected Map<String, DateTime> jobs = new HashMap<String, DateTime>();
	
	public synchronized void addJob(String job) {
		jobs.put(job, new DateTime());
	}
	
	public synchronized void removeJob(String job) {
		jobs.remove(job);
	}
	
	public synchronized Iterable<String> scan() {
		DateTime now = new DateTime();
		LinkedList<String> result = new LinkedList<String>();
		
		for (String job : jobs.keySet()) {
			if (jobs.get(job).plus(TIMEOUT).compareTo(now) < 0) {
				result.add(job);
			}
		}
		
		return result;
	}
}
