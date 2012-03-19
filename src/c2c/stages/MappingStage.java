package c2c.stages;

import java.math.BigInteger;

import c2c.events.MapDone;
import c2c.api.*;
import c2c.payloads.KeyValue;

import seda.sandStorm.api.*;
import bamboo.api.*;

public final class MappingStage extends MapReduceStage {
	private final ClassLoader classLoader = MappingStage.class.getClassLoader();
	private Mapper mapper;
	
	private static class Collector implements OutputCollector {
		private BigInteger dest;
		private MappingStage ms;
		
		public Collector(MappingStage ms, BigInteger dest) {
			this.ms = ms;
			this.dest = dest;
		}

		@Override
		public void collect(String key, String value) {
			KeyValue p = new KeyValue(key, value);
			ms.dispatchTo(dest, PartitioningStage.app_id, p);
		}
		
	}

	public static final long app_id = bamboo.router.Router
			.app_id(MappingStage.class);

	public MappingStage() throws Exception {
		super(KeyValue.class);
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
		} else {
			BUG("Event " + item + " unknown.");
		}
	}

	/**
	 * Perform the computation and send it back to master
	 * 
	 * @param pay
	 * @param src
	 */
	private void map(KeyValue pay, BambooRouteDeliver x) {
		logger.info("Computing " + pay);
		OutputCollector c = new Collector(this, x.src);
		mapper.map(pay.key, pay.value, c);
		dispatchTo(x.src, PartitioningStage.app_id, new MapDone(x.dest));
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
