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
			BambooRouteDeliver deliver = (BambooRouteDeliver) event;
			if (deliver.payload instanceof KeyValue) {
				KeyValue p = (KeyValue) deliver.payload;
				if (!mappers.containsKey(p.key.domain)) {
					try {
						Mapper m = (Mapper) classLoader.loadClass(p.key.domain)
								.newInstance();
						mappers.put(p.key.domain, m);
					} catch (Exception e) {
						BUG(e);
					}
				}
				map(p, deliver);
			} else {
				BUG("Unknown");
			}
		} else if (event instanceof Dht.PutResp) {
			PutResp resp = (PutResp) event;
			if (resp.result != bamboo_stat.BAMBOO_OK) // TODO better handling
				BUG("Put was unsuccessful. System is overcapacity!");
		} else {
			BUG("Event " + event + " unknown.");
		}
	}

	/**
	 * Perform the computation and inform master the mapping is done.
	 * 
	 * @param pay
	 * @param src
	 */
	private void map(KeyValue pay, BambooRouteDeliver msg) {
		logger.info("Computing " + pay);
		mappers.get(pay.key.domain).map(pay.key.data, pay.value,
				new Collector(pay.key.domain));
		dispatchTo(msg.src, PartitioningStage.app_id, pay.key);
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
