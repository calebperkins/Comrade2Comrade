package c2c.stages;

import java.util.HashMap;
import java.util.Map;

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

	private final Map<String, DhtValues> value_buffer = new HashMap<String, DhtValues>();
	
	// Reducers that are underway
	private final WorkerTable reducers = new WorkerTable();
	
	public PartitioningStage() throws Exception {
		super(MappingFinished.class, Dht.GetResp.class);
	}
	
	private void handleJobStatus(JobStatus status) {
		if (!status.mapper) {
			if (status.done) {
				reducers.remove(status.key);
				// TODO reducing finish
				if (reducers.size() == 0) {
					logger.info("Reducing finished!");
				}
			} else {
				reducers.add(status.key);
			}
		}
	}

	private void handleMappingFinished(MappingFinished event) {
		logger.info("Mapping finished. Collecting values...");
		dispatchGet(KeyPayload.intermediateKeys(event.domain));
	}
	
	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) {
			BambooRouteDeliver deliver = (BambooRouteDeliver) event;
			if (deliver.payload instanceof JobStatus) {
				handleJobStatus((JobStatus) deliver.payload);
			} else {
				BUG("Unknown payload");
			}
		} else if (event instanceof MappingFinished) {
			handleMappingFinished((MappingFinished) event);
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
		final DhtValues total = value_buffer.get(resp.key.domain);
		if (total.hasMore()) {
			dispatchGet(total.key, total.getPlacemark());
			logger.debug("There were more values...");
		} else {
			logger.info("There are " + total.size() + " intermediate keys. Starting reduce stage.");
			dispatch(new ReducingUnderway(total.key.domain, total.size()));
			for (String key : total) {
				KeyPayload redKey = new KeyPayload(total.key.domain, key);
				reducers.add(redKey);
				dispatchTo(redKey.toNode(), ReducingStage.app_id, redKey);
			}
			
			acore.registerTimer(1000, retryFailedReducers);
		}
	}
	
	private Runnable retryFailedReducers = new Runnable() {
		
		@Override
		public void run() {
			for (KeyPayload failed : reducers.getFailed()) {
				reducers.add(failed);
				logger.warn("Reducing " + failed + " failed. Retrying...");
				dispatchTo(failed.toNode(), ReducingStage.app_id, failed);
			}
			acore.registerTimer(1000, retryFailedReducers);
		}
	};

	@Override
	public long getAppID() {
		return app_id;
	}

}
