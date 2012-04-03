package c2c.events;

import java.util.HashSet;
import java.util.Set;

import seda.sandStorm.api.QueueElementIF;
import c2c.payloads.KeyValue;

/**
 * A set of key-value pairs for mapping. TODO: add job descriptor
 * 
 * @author caleb
 * 
 */
public class JobRequest implements QueueElementIF {
	public final Set<KeyValue> pairs = new HashSet<KeyValue>();
	public final String domain;
	
	public JobRequest(String class_name) {
		this.domain = class_name;
	}

	public void add(String key, String value) {
		pairs.add(new KeyValue(domain, key, value));
	}
}