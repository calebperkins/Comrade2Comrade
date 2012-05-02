package c2c.utilities;

import java.util.*;
import java.util.Map.Entry;

import org.joda.time.*;

import c2c.payloads.KeyPayload;

/**
 * Keeps track of remote nodes working on a particular input key.
 * 
 * This need not be synchronized, because only the main thread should be
 * adding/removing/scanning workers in this table!
 * 
 */
public class WorkerTable {
	public static final Duration TIMEOUT = new Duration(30 * 1000);
	private Map<KeyPayload, DateTime> pending = new HashMap<KeyPayload, DateTime>();

	public void add(KeyPayload key) {
		pending.put(key, new DateTime());
	}

	public void remove(KeyPayload key) {
		pending.remove(key);
	}
	
	public int size() {
		return pending.size();
	}

	public Iterable<KeyPayload> getFailed() {
		DateTime now = new DateTime();
		LinkedList<KeyPayload> failed = new LinkedList<KeyPayload>();

		for (Entry<KeyPayload, DateTime> entry : pending.entrySet()) {
			if (entry.getValue().plus(TIMEOUT).compareTo(now) < 0) {
				failed.add(entry.getKey());
			}
		}

		return failed;

	}
}
