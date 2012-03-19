package c2c.stages;

import java.math.BigInteger;
import java.util.*;

import c2c.payloads.*;
import c2c.events.*;

import seda.sandStorm.api.*;
import bamboo.api.*;

/**
 * Collects results from mappers
 * 
 * @author caleb
 * 
 */
public final class PartitioningStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(PartitioningStage.class);
	private Map<String, List<String>> results = new HashMap<String, List<String>>();
	private int expected = 0;
	private int received = 0;

	public PartitioningStage() throws Exception {
		super(ReducerInput.class, MappingUnderway.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
		ostore.util.TypeTable.register_type(MapDone.class);
	}

	private void add(KeyValue pair) {
		if (results.containsKey(pair.key)) {
			results.get(pair.key).add(pair.value);
		} else {
			results.put(pair.key, new ArrayList<String>());
			add(pair);
		}
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) {
			BambooRouteDeliver d = (BambooRouteDeliver) item;
			if (d.payload instanceof KeyValue) {
				add((KeyValue) d.payload);
			} else if (d.payload instanceof MapDone) {
				received++;
				if (expected == received) { // distribute to reducers
					for (String key : results.keySet()) {
						List<String> values = results.get(key);
						ReducerInput payload = new ReducerInput(key, values);
						signalReducer(MapReduceStage.randomNode(), payload);
					}
				}
			} else {
				BUG("Unknown payload:" + d.payload);
			}
		} else if (item instanceof MappingUnderway) {
			logger.info("what");
			expected = ((MappingUnderway) item).expected;
		} else {
			BUG("Event unknown");
		}
	}

	private void signalReducer(BigInteger node, ReducerInput payload) {
		dispatchTo(node, ReducingStage.app_id, payload);
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
