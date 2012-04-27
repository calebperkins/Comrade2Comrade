package c2c.utilities;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import c2c.payloads.KeyPayload;
import c2c.payloads.KeyValue;
import c2c.payloads.Value;

import ostore.util.QuickSerializable;

import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.StagesInitializedSignal;
import bamboo.api.BambooRouteDeliver;
import bamboo.api.BambooRouteInit;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.util.StandardStage;

import bamboo.db.StorageManager;
import bamboo.dht.Dht;

/**
 * Extends StandardStage to handle registering the stage and events.
 * 
 * Users need only override the constructor and two abstract methods.
 * 
 * @author Caleb Perkins
 * 
 */
public abstract class MapReduceStage extends StandardStage {
	private boolean initialized = false;
	protected final Queue<QueueElementIF> pending_events = new LinkedList<QueueElementIF>();
	protected static final Random rand = new Random();

	/**
	 * Register a stage with one payload and zero or more events.
	 * 
	 * This registers three common events for you.
	 * 
	 * @param payload
	 *            a payload, may be null
	 * @param events
	 *            additional events to subscribe to
	 * @throws Exception
	 */
	protected MapReduceStage(Class<?> payload, Class<?>... events)
			throws Exception {
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
	public final void handleEvent(QueueElementIF item) {
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
			for (QueueElementIF event : pending_events)
				handleOperationalEvent(event);
		} else {
			pending_events.add(item);
		}
	}

	/**
	 * Bamboo needs an identifier to route events to the right stage.
	 * 
	 * @return this stage's application ID
	 */
	protected abstract long getAppID();

	/**
	 * Handles all events post-initialization.
	 * 
	 * @param event
	 *            the event to process
	 */
	protected abstract void handleOperationalEvent(QueueElementIF event);

	/**
	 * Get a random node ID
	 * 
	 * @return a random node ID
	 */
	protected BigInteger randomNode() {
		return bamboo.util.GuidTools.random_guid(rand);
	}

	/**
	 * Routes to a remote node over Bamboo
	 * 
	 * @param dest
	 *            node key
	 * @param app_id
	 *            which stage gets the BambooRouteDeliver
	 * @param payload
	 *            the message
	 */
	public void dispatchTo(BigInteger dest, long app_id,
			QuickSerializable payload) {
		dispatch(new BambooRouteInit(dest, app_id, false, false, payload));
	}

	public void dispatchPut(KeyPayload key, String value,
			boolean allow_duplicates) {
		Value val = new Value(value, allow_duplicates);
		Dht.PutReq req = new Dht.PutReq(key.toNode(), val.toByteBuffer(),
				val.hash(), true, my_sink, new KeyValue(key, value), 600,
				my_node_id.address());
		dispatch(req);
	}

	/***
	 * Request a GET for a key. The response can be captured by listening to the
	 * Dht.GetResp event. The key will be stored in the user_data attribute on
	 * the response.
	 * 
	 * @param kp
	 */
	public void dispatchGet(KeyPayload kp) {
		dispatchGet(kp, null);
	}

	public void dispatchGet(KeyPayload kp, StorageManager.Key placemark) {
		Dht.GetReq req = new Dht.GetReq(kp.toNode(), 999999, true, placemark,
				my_sink, kp, my_node_id);
		dispatch(req);
	}

}
