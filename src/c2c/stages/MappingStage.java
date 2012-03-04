package c2c.stages;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.Queue;

import c2c.payloads.MapPayload;

import seda.sandStorm.api.*;
import bamboo.util.StandardStage;
import bamboo.api.*;

public final class MappingStage extends StandardStage {

	public static final long app_id = bamboo.router.Router
			.app_id(MappingStage.class);
	private boolean initialized = false;
	private final Queue<QueueElementIF> pending_events = new LinkedList<QueueElementIF>();

	public MappingStage() throws Exception {
		super(); // set up logger
		ostore.util.TypeTable.register_type(MapPayload.class);
		event_types = new Class[] { StagesInitializedSignal.class,
				BambooRouteDeliver.class };
	}

	@Override
	public void handleEvent(QueueElementIF item) {
		if (initialized)
			handleOperationalEvent(item);
		else
			handleInitializationEvent(item);
	}

	/**
	 * Request an application ID, and queue all other events until we get one.
	 * 
	 * @param item
	 */
	private void handleInitializationEvent(QueueElementIF item) {
		if (item instanceof StagesInitializedSignal) {
			dispatch(new BambooRouterAppRegReq(app_id, false, false, false,
					my_sink));
		} else if (item instanceof BambooRouterAppRegResp) {
			initialized = true;
			while (!pending_events.isEmpty())
				handleOperationalEvent(pending_events.remove());
		} else {
			pending_events.add(item);
		}
	}

	private void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) { // do the computation
			BambooRouteDeliver deliver = (BambooRouteDeliver) item;
			compute((MapPayload) deliver.payload, deliver.src);
		} else {
			BUG("Event " + item + " unknown.");
		}
	}

	/**
	 * Perform the computation and send it back to master
	 * 
	 * @param pay
	 * @param src
	 */
	private void compute(MapPayload pay, BigInteger sender) {
		logger.info("Computing " + pay);
		// TODO: remove hard-coding of job instructions
		String[] words = pay.value.split("\\s+");
		for (String w : words) {
			MapPayload p = new MapPayload(pay.job_guid, w, "1");
			dispatch(new BambooRouteInit(sender, MasterStage.app_id, false, false, p));
		}
	}

}
