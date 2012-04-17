package c2c.stages;

import java.util.HashMap;
import java.util.Map;
import c2c.api.*;
import c2c.payloads.KeyPayload;
import c2c.payloads.KeyValue;

import seda.sandStorm.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;
import bamboo.dht.Dht.PutResp;
import bamboo.dht.bamboo_stat;

public final class MappingStage extends MapReduceStage {
	private final Map<String, Mapper> mappers = new HashMap<String, Mapper>();
	private final ClassLoader classLoader = MappingStage.class.getClassLoader();

	public static final long app_id = bamboo.router.Router
			.app_id(MappingStage.class);

	public MappingStage() throws Exception {
		super(KeyValue.class, Dht.PutResp.class);
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) { // do the computation
			handleMapRequest((BambooRouteDeliver) event);
		} else if (event instanceof Dht.PutResp) {
			handlePutResp((PutResp) event);
		} else {
			BUG("Event " + event + " unknown.");
		}
	}

	private void initMapper(String domain) {
		if (!mappers.containsKey(domain)) {
			try {
				Mapper m = (Mapper) classLoader.loadClass(domain).newInstance();
				mappers.put(domain, m);
			} catch (Exception e) {
				BUG(e);
			}
		}
	}

	private void handleMapRequest(BambooRouteDeliver event) {
		KeyValue kv = (KeyValue) event.payload;
		initMapper(kv.key.domain);
		logger.info("Computing " + kv);
		mappers.get(kv.key.domain).map(kv.key.data, kv.value,
				new Collector(kv.key.domain));
		dispatchTo(event.src, PartitioningStage.app_id, kv.key);
	}

	private void handlePutResp(Dht.PutResp response) {
		if (response.result != bamboo_stat.BAMBOO_OK) // TODO better handling
			BUG("Put was unsuccessful. System is overcapacity!");
	}

	@Override
	public long getAppID() {
		return app_id;
	}

	private class Collector implements OutputCollector {
		private String domain;
		private KeyPayload inter;

		public Collector(String domain) {
			this.domain = domain;
			inter = intermediateKeys(domain);
		}

		@Override
		public void collect(String key, String value) {
			dispatchPut(new KeyPayload(domain, key), value, true);
			dispatchPut(inter, key, false);
		}
	}

}
