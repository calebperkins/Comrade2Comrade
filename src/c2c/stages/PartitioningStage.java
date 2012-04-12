package c2c.stages;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import c2c.payloads.*;
import c2c.utilities.DhtValues;
import c2c.events.*;

import seda.sandStorm.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;

/**
 * Waits until mappers are done and initiates reducing. This stage runs on the
 * same node as MasterStage!
 * 
 * @author Caleb Perkins
 * 
 */
public final class PartitioningStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(PartitioningStage.class);
	private final Map<String, Integer> expected = new HashMap<String, Integer>();

	// What mappers for an original input key have completed
	private final Map<String, Set<String>> completed = new HashMap<String, Set<String>>();

	public PartitioningStage() throws Exception {
		super(KeyPayload.class, MappingUnderway.class, Dht.GetResp.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
		ostore.util.TypeTable.register_type(KeyPayload.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) {
			KeyPayload k = (KeyPayload) ((BambooRouteDeliver) event).payload;
			completed.get(k.domain).add(k.data);
			if (completed.get(k.domain).size() == expected.get(k.domain)) { // Mapping
																			// is
																			// done.
																			// Start
																			// reducing.
				dispatchGet(intermediateKeys(k.domain));
			}
		} else if (event instanceof MappingUnderway) {
			MappingUnderway mapping = (MappingUnderway) event;
			expected.put(mapping.domain, mapping.expected);
			completed.put(mapping.domain, new HashSet<String>());
		} else if (event instanceof Dht.GetResp) {
			Dht.GetResp resp = (Dht.GetResp) event;
			KeyPayload kp = (KeyPayload) resp.user_data;
			logger.info(kp + " has " + resp.values.size() + " values.");
			for (String key : new DhtValues(resp)) {
				KeyPayload redKey = new KeyPayload(kp.domain, key);
				dispatchTo(redKey.toNode(), ReducingStage.app_id, redKey);
			}
		} else {
			BUG("Event unknown");
		}
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
