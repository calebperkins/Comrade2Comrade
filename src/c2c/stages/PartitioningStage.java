package c2c.stages;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.AsyncAppender;

import c2c.payloads.*;
import c2c.utilities.DhtValues;
import c2c.utilities.MapReduceStage;
import c2c.utilities.WorkerTable;
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

	// Reducers that are underway
	private final WorkerTable reducers = new WorkerTable();
	
	public PartitioningStage() throws Exception {
		super(MappingUnderway.class, Dht.GetResp.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
		ostore.util.TypeTable.register_type(KeyPayload.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) {
			BambooRouteDeliver deliver = (BambooRouteDeliver) event;
			if (deliver.payload instanceof KeyPayload) {
				KeyPayload k = (KeyPayload) deliver.payload;
				completed.get(k.domain).add(k.data);
				// Mapping is done. Start reducing.
				if (completed.get(k.domain).size() == expected.get(k.domain)) {
					dispatchGet(KeyPayload.intermediateKeys(k.domain));
				}
			} else if (deliver.payload instanceof JobStatus) {
				JobStatus status = (JobStatus) deliver.payload;
				if (!status.mapper) {
					if (status.done) {
						reducers.removeJob(status.domain);
					} else {
						reducers.addJob(status.domain);
					}
				}
			}
		} else if (event instanceof MappingUnderway) {
			MappingUnderway mapping = (MappingUnderway) event;
			expected.put(mapping.domain, mapping.expected);
			completed.put(mapping.domain, new HashSet<String>());
		} else if (event instanceof Dht.GetResp) {
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
			logger.debug("There were more values...");
		} else {
			logger.info("There are " + total.size() + " intermediate keys. Starting reduce stage.");
			dispatch(new ReducingUnderway(total.key.domain, total.size()));
			for (String key : total) {
				KeyPayload redKey = new KeyPayload(total.key.domain, key);
				reducers.addJob(total.key.domain + "::" + key);
				dispatchTo(redKey.toNode(), ReducingStage.app_id, redKey);
				
				acore.register_timer(1000, new Runnable() {
					public void run() {
						for (String failed : reducers.scan()) {
							String[] dandk = failed.split("::");
							
							KeyPayload redKey = new KeyPayload(dandk[0], dandk[1]);
							reducers.addJob(failed);
							dispatchTo(redKey.toNode(), ReducingStage.app_id, redKey);
							acore.register_timer(1000, this);
						}
					}
				});
			}
		}
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
