package c2c.stages;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import ostore.util.QuickSerializable;

import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.StagesInitializedSignal;
import bamboo.api.BambooRouteDeliver;
import bamboo.api.BambooRouteInit;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.util.StandardStage;

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
	private final Queue<QueueElementIF> pending_events = new LinkedList<QueueElementIF>();
	protected static final Random rand = new Random();
	private static final Charset charset = Charset.forName("UTF-8");

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
			while (!pending_events.isEmpty())
				handleOperationalEvent(pending_events.remove());
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
	 * @param item
	 *            the event to process
	 */
	protected abstract void handleOperationalEvent(QueueElementIF item);

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

	public void requestPut(String key, String value, boolean allow_duplicates) {
		if (allow_duplicates) // dirty hack
			value = value + ":::" + rand.nextInt();
		else
			value = value + ":::0";
		BigInteger k = nodeFromKey(key);
		ByteBuffer v = ByteBuffer.wrap(value.getBytes(charset));
		byte[] vh = BigInteger.valueOf(value.hashCode()).toByteArray();
		Dht.PutReq req = new Dht.PutReq(k, v, vh, true, my_sink, null,
				Dht.MAX_TTL_SEC, my_node_id.address());
		dispatch(req);
	}

	/***
	 * Request a GET for a key. The response can be captured by listening to the
	 * Dht.GetResp event. The key will be stored in the user_data attribute on
	 * the response.
	 * 
	 * @param key
	 */
	public void requestGet(String key) {
		BigInteger k = nodeFromKey(key);
		Dht.GetReq req = new Dht.GetReq(k, 1000, true, null, my_sink, key,
				my_node_id);
		dispatch(req);
	}

	public BigInteger nodeFromKey(String key) {
		return new BigInteger(key.getBytes(charset));
	}

	/**
	 * Makes parsing a GET value cleaner, because Bamboo does not use generics
	 * for the Dht.GetResp values yet, and they are ByteBuffers.
	 * 
	 * @author Caleb Perkins
	 * 
	 */
	private static class GetRespIterator implements Iterator<String> {
		private Iterator<Dht.GetValue> raw;
		private static final CharsetDecoder decoder = charset.newDecoder();

		@SuppressWarnings("unchecked")
		public GetRespIterator(Dht.GetResp resp) {
			raw = resp.values.iterator();
		}

		@Override
		public boolean hasNext() {
			return raw.hasNext();
		}

		@Override
		public String next() {
			ByteBuffer buffer = raw.next().value;
			try {
				String data = decoder.decode(buffer).toString();

				// dirty hack to allow duplicates
				return data.split(":::")[0];
			} catch (CharacterCodingException e) {
				// TODO handle this better. Should be a fatal error?
				System.err.println(e);
				return "";
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	public Iterator<String> parseGetResp(Dht.GetResp resp) {
		return new GetRespIterator(resp);
	}
}
