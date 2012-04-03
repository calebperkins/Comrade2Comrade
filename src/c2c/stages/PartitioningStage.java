package c2c.stages;

import java.util.HashMap;
import java.util.Map;

import c2c.payloads.*;
import c2c.events.*;

import seda.sandStorm.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;

/**
 * Waits until mappers are done and initiates reducing.
 * 
 * @author Caleb Perkins
 * 
 */
public final class PartitioningStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(PartitioningStage.class);
	private final Map<String, Integer> remaining = new HashMap<String, Integer>();

	public PartitioningStage() throws Exception {
		super(KeyPayload.class, MappingUnderway.class, Dht.GetResp.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
		ostore.util.TypeTable.register_type(KeyPayload.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) {
			KeyPayload k = (KeyPayload) ((BambooRouteDeliver) item).payload;
			int remain = remaining.get(k.domain);
			remain--;
			remaining.put(k.domain, remain);
			if (remain == 0) { // Mapping is done. Start reducing.
				dispatchGet(k.domain, "i");
			}
		} else if (item instanceof MappingUnderway) {
			MappingUnderway event = (MappingUnderway) item;
			remaining.put(event.domain, event.expected);
		} else if (item instanceof Dht.GetResp) {
			Dht.GetResp resp = (Dht.GetResp) item;
			KeyPayload kp = (KeyPayload) resp.user_data;
			logger.info(kp + " has " + resp.values.size() + " values.");
			for (String key : parseGetResp(resp)) {
				dispatchTo(nodeFromKey(key), ReducingStage.app_id,
						new KeyPayload(kp.domain, key));
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
