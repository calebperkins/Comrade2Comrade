package c2c.stages;

import c2c.api.*;
import c2c.payloads.KeyPayload;
import c2c.payloads.KeyValue;

import seda.sandStorm.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;

public final class MappingStage extends MapReduceStage implements
		OutputCollector {
	private final ClassLoader classLoader = MappingStage.class.getClassLoader();
	private Mapper mapper;

	public static final long app_id = bamboo.router.Router
			.app_id(MappingStage.class);

	public MappingStage() throws Exception {
		super(KeyValue.class, Dht.PutResp.class);
	}

	@Override
	public void init(ConfigDataIF config) throws Exception {
		super.init(config);
		String mapper_name = config_get_string(config, "mapper");
		mapper = (Mapper) classLoader.loadClass(mapper_name).newInstance();
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) { // do the computation
			BambooRouteDeliver deliver = (BambooRouteDeliver) item;
			map((KeyValue) deliver.payload, deliver);
		} else if (item instanceof Dht.PutResp) {
			// TODO check response
		} else {
			BUG("Event " + item + " unknown.");
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
		mapper.map(pay.key, pay.value, this);
		dispatchTo(x.src, PartitioningStage.app_id, new KeyPayload(pay.key));
	}

	@Override
	public long getAppID() {
		return app_id;
	}

	@Override
	public void collect(String key, String value) {
		dispatchPut(key, value, true);
		dispatchPut("intermediate-keys", key, false);
	}

}
