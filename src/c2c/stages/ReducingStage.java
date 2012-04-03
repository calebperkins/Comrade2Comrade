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

public final class ReducingStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(ReducingStage.class);
	private final ClassLoader classLoader = ReducingStage.class
			.getClassLoader();
	private Map<String, Reducer> reducers = new HashMap<String, Reducer>();

	public ReducingStage() throws Exception {
		super(KeyPayload.class, Dht.GetResp.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) {
			BambooRouteDeliver deliver = (BambooRouteDeliver) item;
			KeyPayload payload = (KeyPayload) deliver.payload;
			dispatchGet(payload.domain, payload.key);
			if (!reducers.containsKey(payload.domain)) {
				try {
					Reducer r = (Reducer) classLoader.loadClass(payload.domain).newInstance();
					reducers.put(payload.domain, r);
				} catch (Exception e) {
					BUG(e);
				}
			}
		} else if (item instanceof Dht.GetResp) {
			Dht.GetResp resp = (GetResp) item;
			KeyPayload k = (KeyPayload) resp.user_data;
			reducers.get(k.domain).reduce(k.key, parseGetResp(resp), new Collector(k.domain));
		} else {
			BUG("Unexpected event:" + item);
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
