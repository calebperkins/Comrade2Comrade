package c2c.stages;

import c2c.events.MapDone;
import c2c.payloads.MapPair;

import seda.sandStorm.api.*;
import bamboo.api.*;

public final class MappingStage extends MapReduceStage {

	public static final long app_id = bamboo.router.Router
			.app_id(MappingStage.class);

	public MappingStage() throws Exception {
		super(MapPair.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) { // do the computation
			BambooRouteDeliver deliver = (BambooRouteDeliver) item;
			map((MapPair) deliver.payload, deliver);
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
	private void map(MapPair pay, BambooRouteDeliver x) {
		logger.info("Computing " + pay);
		// TODO: remove hard-coding of job instructions
		String[] words = pay.value.split("\\s+");
		for (String w : words) {
			MapPair p = new MapPair(w, "1");
			dispatch(new BambooRouteInit(x.src, PartitioningStage.app_id,
					false, false, p));
		}
		dispatch(new BambooRouteInit(x.src, PartitioningHelper.app_id, false,
				false, new MapDone(x.dest)));
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
