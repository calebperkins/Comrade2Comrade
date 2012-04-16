package c2c.stages;

import seda.sandStorm.api.*;
import c2c.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;
import bamboo.dht.Dht.GetResp;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import c2c.payloads.KeyValue;
import c2c.payloads.KeyPayload;
import c2c.utilities.DhtValues;

public final class ReducingStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(ReducingStage.class);
	private final ClassLoader classLoader = ReducingStage.class
			.getClassLoader();
	private Map<String, Reducer> reducers = new HashMap<String, Reducer>();
	private Map<KeyPayload, DhtValues> responses = new HashMap<KeyPayload, DhtValues>();

	public ReducingStage() throws Exception {
		super(KeyPayload.class, Dht.GetResp.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) {
			BambooRouteDeliver deliver = (BambooRouteDeliver) event;
			KeyPayload payload = (KeyPayload) deliver.payload;
			handleReduceRequest(payload);
		} else if (event instanceof Dht.GetResp) {
			handleGetResponse((GetResp) event);
		} else {
			BUG("Unexpected event:" + event);
		}
	}

	private void handleReduceRequest(KeyPayload payload) {
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
			reducers.get(total.key.domain).reduce(total.key.data, total,
					new Collector(total.key.domain));
			dispatchTo(BigInteger.ZERO, MasterStage.app_id, total.key);
		}
	}

	@Override
	public long getAppID() {
		return app_id;
	}

	private class Collector implements OutputCollector {
		private String domain;

		public Collector(String domain) {
			this.domain = domain;
		}

		@Override
		public void collect(String key, String value) {
			KeyValue p = new KeyValue(domain, key, value);
			dispatchTo(BigInteger.ZERO, MasterStage.app_id, p); // FIXME wrong
																// node ID
		}
	}

}
