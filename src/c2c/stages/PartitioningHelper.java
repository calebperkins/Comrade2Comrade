package c2c.stages;

import bamboo.api.BambooRouteDeliver;
import seda.sandStorm.api.QueueElementIF;
import c2c.events.*;

public class PartitioningHelper extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(PartitioningHelper.class);

	public PartitioningHelper() throws Exception {
		super(null);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) {
			BambooRouteDeliver d= (BambooRouteDeliver) item;
			dispatch((MapDone) d.payload);
		} else {
			BUG("lol");
		}
	}

	@Override
	public long getAppID() {
		return app_id;
	}
	
	

}
