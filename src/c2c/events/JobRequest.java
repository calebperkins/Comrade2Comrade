package c2c.events;

import java.util.HashSet;
import java.util.Set;

import seda.sandStorm.api.QueueElementIF;
import c2c.payloads.MapPair;

/**
 * A set of key-value pairs for mapping. TODO: add job descriptor
 * 
 * @author caleb
 * 
 */
public class JobRequest implements QueueElementIF {
	public final Set<MapPair> pairs = new HashSet<MapPair>();

	public void add(String key, String value) {
		pairs.add(new MapPair(key, value));
	}
}