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
import c2c.payloads.JobStatus;
import c2c.payloads.KeyValue;
import c2c.payloads.KeyPayload;
import c2c.payloads.Value;
import c2c.utilities.DhtValues;
import c2c.utilities.MapReduceStage;
import c2c.utilities.RemoteJob;

public final class ReducingStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(ReducingStage.class);
	private Map<KeyPayload, DhtValues> responses = new HashMap<KeyPayload, DhtValues>();

	private final Map<String, RemoteJob> jobs = new HashMap<String, RemoteJob>();

	// Reducer
	private final Map<KeyPayload, Integer> remaining = new HashMap<KeyPayload, Integer>();

	private final ExecutorService pool = Executors.newCachedThreadPool();

	public ReducingStage() throws Exception {
		super(Dht.GetResp.class, Dht.PutResp.class);
	}

	private RemoteJob getJob(String domain, BigInteger master) {
		if (!jobs.containsKey(domain)) {
			try {
				jobs.put(domain, new RemoteJob(domain, master));
			} catch (Exception e) {
				BUG(e);
			}
		}
		return jobs.get(domain);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) {
			handleReduceRequest((BambooRouteDeliver) event);
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
			dispatchTo(jobs.get(kv.key.domain).getMaster(), MasterStage.app_id, new KeyValue(kv.key, kv.value.value)); // TODO: remove
			if (remaining.put(kv.key, remaining.get(kv.key) - 1) == 1) {
				dispatchTo(jobs.get(kv.key.domain).getMaster(), MasterStage.app_id,
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

	private void handleReduceRequest(BambooRouteDeliver deliver) {
		KeyPayload payload = (KeyPayload) deliver.payload;
		getJob(payload.domain, deliver.src);
		logger.info("Reducing " + payload);
		dispatchGet(payload);
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
		final Reducer reducer = jobs.get(total.key.domain).getReducer();
		final BigInteger src = jobs.get(total.key.domain).getMaster();
		
		pool.execute(new Runnable() {
			boolean working = true;
			
			@Override
			public void run() {
				final Collector c = new Collector(total.key);
				
				acore.registerTimer(10, new Runnable() {
					@Override
					public void run() {
						if (working) {
							dispatchTo(src, PartitioningStage.app_id, 
									new JobStatus(total.key, false, false));
							acore.registerTimer(1000, this);
						}
					}
				});
				
				reducer.reduce(total.key.data, total, c);

				// Get back to main thread
				acore.registerTimer(0, new Runnable() {

					@Override
					public void run() {
						working = false;
						dispatchTo(src, PartitioningStage.app_id, new JobStatus(total.key, true, false));
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
