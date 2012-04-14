package c2c.stages;

import ostore.network.NetworkMessage;
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

	public MasterStage() throws Exception {
		super(KeyValue.class, JobRequest.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) { // get back the results
			BambooRouteDeliver deliver = (BambooRouteDeliver) event;
			logger.info("Results back: " + deliver.payload);
		} else if (event instanceof JobRequest) { // Distribute jobs to mappers.
			JobRequest req = (JobRequest) event;
			dispatch(new MappingUnderway(req.domain, req.pairs.size()));
			for (KeyValue pair : req.pairs) {
				// Distribute to different nodes.
				dispatchTo(pair.key.toNode(), MappingStage.app_id,
						pair);
			}
		} else if (event instanceof NetworkMessage) {
			NetworkMessage m = (NetworkMessage) event;
			logger.fatal(m);
		} else {
			BUG("Event " + event + " unknown.");
		}
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
