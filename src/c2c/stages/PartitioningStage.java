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
	
	private final Map<String, DhtValues> value_buffer = new HashMap<String, DhtValues>();

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
			// Mapping is done. Start reducing.
			if (completed.get(k.domain).size() == expected.get(k.domain)) {
				dispatchGet(intermediateKeys(k.domain));
			}
		} else if (event instanceof MappingUnderway) {
			MappingUnderway mapping = (MappingUnderway) event;
			expected.put(mapping.domain, mapping.expected);
			completed.put(mapping.domain, new HashSet<String>());
		} else if (event instanceof Dht.GetResp) {
			/*Dht.GetResp resp = (Dht.GetResp) event;
			KeyPayload kp = (KeyPayload) resp.user_data;
			logger.info(kp + " has " + resp.values.size() + " values.");
			dispatch(new ReducingUnderway(kp.domain, resp.values.size()));
			DhtValues x = new DhtValues(resp);
			for (String key : x) {
				KeyPayload redKey = new KeyPayload(kp.domain, key);
				dispatchTo(redKey.toNode(), ReducingStage.app_id, redKey);
			}
			if (x.hasMore())
				BUG("NEED MORE");*/
			handleIntermediateValues((Dht.GetResp) event);
		} else {
			BUG("Event unknown");
		}
	}
	
	private void handleIntermediateValues(Dht.GetResp response) {
		DhtValues resp = new DhtValues(response);
		if (value_buffer.containsKey(resp.key.domain)) {
			value_buffer.get(resp.key.domain).append(resp);
		} else {
			value_buffer.put(resp.key.domain, resp);
		}
		DhtValues total = value_buffer.get(resp.key.domain);
		if (total.hasMore()) {
			dispatchGet(total.key, total.getPlacemark());
			logger.info("There were more values...");
		} else {
			logger.info("There were " + total.size());
			dispatch(new ReducingUnderway(total.key.domain, total.size()));
			for (String key : total) {
				KeyPayload redKey = new KeyPayload(total.key.domain, key);
				dispatchTo(redKey.toNode(), ReducingStage.app_id, redKey);
			}
		}
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
