package c2c.stages;

import java.util.HashMap;
import java.util.Map;
import c2c.api.*;
import c2c.events.CodeRequest;
import c2c.payloads.ClassPayload;
import c2c.payloads.KeyPayload;
import c2c.payloads.KeyValue;

import seda.sandStorm.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;
import bamboo.dht.Dht.PutResp;

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
				if (!mappers.containsKey(p.domain)) {
					try {
						Mapper m = (Mapper) classLoader.loadClass(p.domain).newInstance();
						mappers.put(p.domain, m);
					} catch (ClassNotFoundException e) { // ask sender for class code
						dispatch(new CodeRequest(deliver.immediate_src));
					} catch (Exception e) {
						BUG(e);
					}				
				}
				map(p, deliver);
			} else if (deliver.payload instanceof ClassPayload) {
				ClassPayload p = (ClassPayload) deliver.payload;
				try {
					mappers.put(p.name, (Mapper) p.toClass().newInstance());
					
					for (QueueElementIF e : pending_events) {
						handleOperationalEvent(e);
					}
				} catch (Exception e) {
					BUG(e);
				}
			}
			
		} else if (event instanceof Dht.PutResp) {
			PutResp resp = (PutResp) event;
			if (resp.result != 0) // TODO better handling
				BUG("Put was unsuccessful.");
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
	private void map(KeyValue pay, BambooRouteDeliver x) {
		logger.info("Computing " + pay);
		mappers.get(pay.domain).map(pay.key, pay.value, new Collector(pay.domain));
		dispatchTo(x.src, PartitioningStage.app_id, new KeyPayload(pay.domain, pay.key));
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
			dispatchPut(domain, key, value, true);
			dispatchPut(domain, "i", key, false);
		}
	}	

}
