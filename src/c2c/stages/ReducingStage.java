package c2c.stages;

import seda.sandStorm.api.*;
import c2c.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;
import bamboo.dht.bamboo_stat;
import bamboo.dht.Dht.GetResp;
import bamboo.dht.Dht.PutResp;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import c2c.payloads.IntermediateKeyValue;
import c2c.payloads.KeyValue;
import c2c.payloads.KeyPayload;
import c2c.payloads.Value;
import c2c.utilities.DhtValues;
import c2c.utilities.MapReduceStage;

public final class ReducingStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(ReducingStage.class);
	private final ClassLoader classLoader = ReducingStage.class
			.getClassLoader();
	private Map<String, Reducer> reducers = new HashMap<String, Reducer>();
	private Map<KeyPayload, DhtValues> responses = new HashMap<KeyPayload, DhtValues>();
	private Map<String, BigInteger> masters = new HashMap<String, BigInteger>();
	
	// Reducer 
	private final Map<KeyPayload, Integer> remaining = new HashMap<KeyPayload, Integer>();
	
	private final ExecutorService pool = Executors.newCachedThreadPool();

	public ReducingStage() throws Exception {
		super(KeyPayload.class, Dht.GetResp.class, Dht.PutResp.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) {
			BambooRouteDeliver deliver = (BambooRouteDeliver) event;
			KeyPayload payload = (KeyPayload) deliver.payload;
			masters.put(payload.domain, deliver.src);
			handleReduceRequest(payload);
		} else if (event instanceof Dht.GetResp) {
			handleGetResponse((GetResp) event);
		} else if (event instanceof Dht.PutResp) {
			handlePutResponse((PutResp) event);
		} else {
			BUG("Unexpected event:" + event);
		}
	}
	
	private void handlePutResponse(Dht.PutResp resp) {
		IntermediateKeyValue kv = (IntermediateKeyValue) resp.user_data;
		if (resp.result == bamboo_stat.BAMBOO_OK) {
			dispatchTo(masters.get(kv.key.domain), MasterStage.app_id, new KeyValue(kv.key, kv.value.value)); // TODO: remove
			if (remaining.put(kv.key, remaining.get(kv.key) - 1) == 1) {
				dispatchTo(masters.get(kv.key.domain), MasterStage.app_id,
						kv.key); // tell master we are done
			}
		} else {
			logger.info("Reducer put failed. Retrying...");
			Value v = kv.value;
			Dht.PutReq req = new Dht.PutReq(kv.key.toNode(), v.toByteBuffer(),
					v.hash(), true, my_sink, kv, 600,
					my_node_id.address());
			classifier.dispatch_later(req, 5000);
		}
	}

	private void handleReduceRequest(KeyPayload payload) {
		logger.info("Reducing " + payload);
		dispatchGet(payload);
		if (!reducers.containsKey(payload.domain)) {
			try {
				Reducer r = (Reducer) classLoader.loadClass(payload.domain)
						.newInstance();
				reducers.put(payload.domain, r);
			} catch (Exception e) {
				BUG(e);
			}
		}
	}

	private void handleGetResponse(GetResp event) {
		DhtValues resp = new DhtValues(event);
		if (responses.containsKey(resp.key)) {
			responses.get(resp.key).append(resp);
		} else {
			responses.put(resp.key, resp);
		}
		DhtValues total = responses.get(resp.key);
		if (total.hasMore()) {
			dispatchGet(total.key, total.getPlacemark());
		} else {
			performReduce(total);
		}
	}
	
	/**
	 * Start a new thread because user's reduce may be blocking
	 * @param total
	 */
	private void performReduce(final DhtValues total) {
		final Reducer reducer = reducers.get(total.key.domain);
		pool.execute(new Runnable() {
			
			@Override
			public void run() {
				final Collector c = new Collector(total.key);
				reducer.reduce(total.key.data, total, c);
				
				// Get back to main thread
				acore.registerTimer(0, new Runnable() {
					
					@Override
					public void run() {
						c.flush();
					}
				});
			}
		});
	}

	@Override
	public long getAppID() {
		return app_id;
	}

	private class Collector implements OutputCollector {
		private KeyPayload input;
		private Collection<KeyValue> buffer = new LinkedList<KeyValue>();

		public Collector(KeyPayload input) {
			this.input = input;
		}
		
		public void flush() {
			remaining.put(input, buffer.size());
			for (KeyValue kv : buffer) {
				Value v = new Value(kv.value, false);
				Dht.PutReq req = new Dht.PutReq(kv.key.toNode(), v.toByteBuffer(), v.hash(), true, my_sink, new IntermediateKeyValue(input, kv.key, v), 600, my_node_id.address());
				dispatch(req);
			}
		}

		@Override
		public void collect(String key, String value) {
			buffer.add(new KeyValue(input.domain, key, value));
		}
	}

}
