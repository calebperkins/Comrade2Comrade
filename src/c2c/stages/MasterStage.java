package c2c.stages;

import java.util.HashMap;
import java.util.Map;

import seda.sandStorm.api.QueueElementIF;

import c2c.events.*;
import c2c.payloads.*;
import c2c.utilities.LocalJob;
import c2c.utilities.MapReduceStage;
import c2c.utilities.WorkerTable;

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
	private final WorkerTable mappers = new WorkerTable();

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
				logger.info("Job done for " + status.key);
				if (keyvalues.isEmpty()) { // TODO
					dispatch(new MappingFinished(status.key.domain));
				}
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
			logger.debug("Rescanning....");
			
			// Re-dispatch all failed jobs 
			for (KeyPayload failed : mappers.getFailed()) {
				
				logger.warn("Mapping failed for " + failed);
				
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
