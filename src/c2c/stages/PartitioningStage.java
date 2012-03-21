package c2c.stages;

import java.util.*;

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
	private int expected = 0;
	private int received = 0;

	public PartitioningStage() throws Exception {
		super(ReducerInput.class, MappingUnderway.class, Dht.GetResp.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
		ostore.util.TypeTable.register_type(MapDone.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) {
			received++;
			if (expected == received) { // Mapping is done. Start reducing.
				requestGet("intermediate-keys");
			}
		} else if (item instanceof MappingUnderway) {
			expected = ((MappingUnderway) item).expected;
		} else if (item instanceof Dht.GetResp) {
			Iterator<String> keys = parseGetResp((Dht.GetResp) item);
			while (keys.hasNext()) {
				String key = keys.next();
				dispatchTo(nodeFromKey(key), ReducingStage.app_id,
						new ReducerInput(key));
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
