package c2c.stages;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import seda.sandStorm.api.QueueElementIF;

import c2c.events.*;
import c2c.payloads.*;

import bamboo.api.*;

/**
 * Takes job requests from a Client and disperses them to mappers.
 * 
 * @author Caleb Perkins
 * 
 */
public final class MasterStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(MasterStage.class);
	
	private final Map<String, Integer> expected = new HashMap<String, Integer>();
	private final Map<String, Set<String>> completed = new HashMap<String, Set<String>>();

	public MasterStage() throws Exception {
		super(KeyValue.class, JobRequest.class, ReducingUnderway.class);
		ostore.util.TypeTable.register_type(KeyPayload.class);
	}
	
	private void handleReducerDone(KeyPayload k) {
		logger.debug("Reducer done for " + k);
		Set<String> comp = completed.get(k.domain);
		comp.add(k.data);
		if (comp.size() == expected.get(k.domain)) {
			dispatch(new JobDone(k.domain));
		}
	}
	
	private void handleResultBack(KeyValue kv) {
		logger.debug("Result back: " + kv);
		dispatch(kv);
	}
	
	private void handleReducingUnderway(ReducingUnderway event) {
		expected.put(event.domain, event.reducers);
		completed.put(event.domain, new HashSet<String>());
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) { // get back the results
			BambooRouteDeliver deliver = (BambooRouteDeliver) event;
			if (deliver.payload instanceof KeyValue) {
				handleResultBack((KeyValue) deliver.payload);
			} else if (deliver.payload instanceof KeyPayload) {
				handleReducerDone((KeyPayload) deliver.payload);
			} else {
				BUG("Unknown payload.");
			}	
		} else if (event instanceof JobRequest) { // Distribute jobs to mappers.
			JobRequest req = (JobRequest) event;
			dispatch(new MappingUnderway(req.domain, req.pairs.size()));
			for (KeyValue pair : req.pairs) {
				// Distribute to different nodes.
				dispatchTo(pair.key.toNode(), MappingStage.app_id,
						pair);
			}
		} else if (event instanceof ReducingUnderway) {
			handleReducingUnderway((ReducingUnderway) event);
		} else {
			BUG("Event " + event + " unknown.");
		}
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
