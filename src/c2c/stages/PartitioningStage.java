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
		super(ReducerInput.class, MappingUnderway.class, MapDone.class);
		ostore.util.TypeTable.register_type(MapPair.class);
	}

	private void add(MapPair pair) {
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
			add((MapPair) d.payload);
		} else if (item instanceof MapDone) {
			logger.info("got it");
			received++;
			if (expected == received) { // distribute to reducers
				for (String key : results.keySet()) {
					List<String> values = results.get(key);
					ReducerInput payload = new ReducerInput(key, values);
					signalReducer(MapReduceStage.randomNode(), payload);
				}
			}
		} else if (item instanceof MappingUnderway) {
			logger.info("what");
			expected = ((MappingUnderway) item).expected;
		} else {
			BUG("Event unknown");
		}
	}

	private void signalReducer(BigInteger node, ReducerInput payload) {
		dispatch(new BambooRouteInit(node, ReducingStage.app_id, false, false,
				payload));
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
