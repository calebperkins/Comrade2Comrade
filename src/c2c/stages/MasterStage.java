package c2c.stages;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import seda.sandStorm.api.QueueElementIF;

import c2c.events.*;
import c2c.payloads.*;
import c2c.utilities.LocalJob;
import c2c.utilities.MapReduceStage;

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
	
	// KV's have to be stored in case we need to reissue a job
	private Map<KeyPayload, String> keyvalues = new HashMap<KeyPayload, String>();
	
	// When was the last time we heard from a worker?
	private Mappings mappers = new Mappings();
	
	protected static final Duration MAPPER_TIMEOUT = new Duration(10 * 1000);
	
	private static class Mappings {
		private Map<KeyPayload, DateTime> pending = new HashMap<KeyPayload, DateTime>();
		
		public void add(KeyPayload key) {
			pending.put(key, new DateTime());
		}
		
		public void remove(KeyPayload key) {
			pending.remove(key);
		}
		
		public Iterable<KeyPayload> getFailed() {
			DateTime now = new DateTime();
			LinkedList<KeyPayload> failed = new LinkedList<KeyPayload>();
			for (Entry<KeyPayload, DateTime> entry : pending.entrySet()) {
				if (entry.getValue().plus(MAPPER_TIMEOUT).compareTo(now) < 0) {
					failed.add(entry.getKey());
				}
			}
			return failed;
		}
	}

	public MasterStage() throws Exception {
		super(JobRequest.class, ReducingUnderway.class);
	}
	
	private void handleReducerDone(KeyPayload k) {
		logger.debug("Reducer done for " + k);
		LocalJob job = LocalJob.get(k.domain);
		job.reductionDoneFor(k.data);
		if (job.reducingComplete()) {
			dispatch(new JobDone(k.domain));
		}
	}
	
	private void handleResultBack(KeyValue kv) {
		logger.debug("Result back: " + kv);
		dispatch(kv);
	}
	
	private void handleReducingUnderway(ReducingUnderway event) {
		LocalJob.get(event.domain).nowReducing(event.reducers);
	}
	
	private void handleJobRequest(JobRequest req) {
		LocalJob.put(req.domain, new LocalJob(req.pairs));
		dispatch(new MappingUnderway(req.domain, req.pairs.size()));
		
		for (KeyValue pair : req.pairs) {
			keyvalues.put(pair.key, pair.value);
			mappers.add(pair.key);
			
			// Distribute to different nodes.
			dispatchTo(pair.key.toNode(), MappingStage.app_id,
					pair);
		}
		
		// Schedule rescan of worker table
		acore.registerTimer(4500, rescanTable);
	}
	
	private void handleJobStatus(JobStatus status) {
		if (status.mapper) {
			if (status.done) {
				// Mapper is done - remove from tables
				mappers.remove(status.key);
				keyvalues.remove(status.key);
			} else {
				// Mapper still working - refresh in table
				mappers.add(status.key);
			}
		}
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) { // get back the results
			BambooRouteDeliver deliver = (BambooRouteDeliver) event;
			if (deliver.payload instanceof KeyValue) {
				handleResultBack((KeyValue) deliver.payload);
			} else if (deliver.payload instanceof KeyPayload) {
				handleReducerDone((KeyPayload) deliver.payload);
			} else if (deliver.payload instanceof JobStatus) {
				handleJobStatus((JobStatus) deliver.payload);
			} else {
				BUG("Unknown payload.");
			}	
		} else if (event instanceof JobRequest) { // Distribute jobs to mappers.
			handleJobRequest((JobRequest) event);
		} else if (event instanceof ReducingUnderway) {
			handleReducingUnderway((ReducingUnderway) event);
		} else {
			BUG("Event " + event + " unknown.");
		}
	}
	
	private Runnable rescanTable = new Runnable() {
		@Override
		public void run() {
			// Re-dispatch all failed jobs 
			for (KeyPayload failed : mappers.getFailed()) {
				KeyValue kv = new KeyValue(failed, keyvalues.get(failed));
				
				// Re-add as current
				mappers.add(failed);
				dispatchTo(failed.toNode(), MappingStage.app_id, kv);
			}
			
			// Schedule next rescan
			acore.registerTimer(4500, rescanTable);
		}
	};

	@Override
	public long getAppID() {
		return app_id;
	}

}
