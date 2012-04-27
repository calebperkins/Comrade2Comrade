package c2c.stages;

import seda.sandStorm.api.*;
import c2c.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;
import bamboo.dht.Dht.GetResp;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import c2c.payloads.KeyValue;
import c2c.payloads.KeyPayload;
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
	
	private final ExecutorService pool = Executors.newCachedThreadPool();

	public ReducingStage() throws Exception {
		super(Dht.GetResp.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
		ostore.util.TypeTable.register_type(KeyPayload.class);
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
		} else {
			BUG("Unexpected event:" + event);
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
				final Collector c = new Collector(total.key.domain);
				reducer.reduce(total.key.data, total, c);
				
				// Get back to main thread
				acore.registerTimer(0, new Runnable() {
					
					@Override
					public void run() {
						c.flush();
						dispatchTo(masters.get(total.key.domain), MasterStage.app_id,
								total.key); // tell master we are done
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
		private String domain;
		private Collection<KeyValue> buffer = new LinkedList<KeyValue>();

		public Collector(String domain) {
			this.domain = domain;
		}
		
		public void flush() {
			for (KeyValue kv : buffer)
				dispatchTo(masters.get(domain), MasterStage.app_id, kv);
		}

		@Override
		public void collect(String key, String value) {
			buffer.add(new KeyValue(domain, key, value));
		}
	}

}
