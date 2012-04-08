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
			dispatchGet(payload);
			if (!reducers.containsKey(payload.domain)) {
				try {
					Reducer r = (Reducer) classLoader.loadClass(payload.domain).newInstance();
					reducers.put(payload.domain, r);
				} catch (Exception e) {
					BUG(e);
				}
			}
		} else if (event instanceof Dht.GetResp) {
			Dht.GetResp resp = (GetResp) event;
			KeyPayload k = (KeyPayload) resp.user_data;
			if (responses.containsKey(k)) {
				responses.get(k).append(resp);
			} else {
				responses.put(k, new DhtValues(resp));
			}
			if (responses.get(k).hasMore()) {
				dispatchGet(k, resp.placemark);
			} else {
				reducers.get(k.domain).reduce(responses.get(k).getKey(), responses.get(k), new Collector(k.domain));
			}
		} else {
			BUG("Unexpected event:" + event);
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
			dispatchTo(BigInteger.ZERO, MasterStage.app_id, p);
		}
	}
	
}
