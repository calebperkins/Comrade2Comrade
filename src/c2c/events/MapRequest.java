package c2c.events;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import seda.sandStorm.api.QueueElementIF;
import c2c.payloads.MapPayload;

/**
 * A set of key-value pairs for mapping. TODO: add job descriptor
 * 
 * @author caleb
 * 
 */
public class MapRequest implements QueueElementIF {
	public final Set<MapPayload> pairs = new HashSet<MapPayload>();
	public final BigInteger guid;
	
	public MapRequest(Random rand) {
		guid = bamboo.util.GuidTools.random_guid(rand);
	}

	public void add(String key, String value) {
		pairs.add(new MapPayload(guid, key, value));
	}
}