package c2c.stages;

import java.util.LinkedList;
import java.util.Queue;

import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.StagesInitializedSignal;
import bamboo.api.BambooRouteDeliver;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.util.StandardStage;

public abstract class MapReduceStage extends StandardStage {
	private boolean initialized = false;
	private final Queue<QueueElementIF> pending_events = new LinkedList<QueueElementIF>();
	
	protected MapReduceStage(Class<?> payload, Class<?>... events) throws Exception {
		super();
		if (payload != null) {
			ostore.util.TypeTable.register_type(payload);
		}
		event_types = new Class[3 + events.length];
		event_types[0] = StagesInitializedSignal.class;
		event_types[1] = BambooRouteDeliver.class;
		event_types[2] = BambooRouterAppRegResp.class;
		for (int i = 0; i < events.length; i++) {
			event_types[i + 3] = events[i];
		}
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
			dispatch(new BambooRouterAppRegReq(getAppID(), false, false, false,
					my_sink));
		} else if (item instanceof BambooRouterAppRegResp) {
			initialized = true;
			while (!pending_events.isEmpty())
				handleOperationalEvent(pending_events.remove());
		} else {
			pending_events.add(item);
		}
	}
	
	public abstract long getAppID();
	protected abstract void handleOperationalEvent(QueueElementIF item);
}
