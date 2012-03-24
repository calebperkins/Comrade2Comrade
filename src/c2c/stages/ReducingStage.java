package c2c.stages;

import seda.sandStorm.api.*;
import c2c.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;
import bamboo.dht.Dht.GetResp;

import java.math.BigInteger;

import c2c.payloads.KeyValue;
import c2c.payloads.KeyPayload;

public final class ReducingStage extends MapReduceStage implements
		OutputCollector {
	public static final long app_id = bamboo.router.Router
			.app_id(ReducingStage.class);
	private final ClassLoader classLoader = ReducingStage.class
			.getClassLoader();
	private Reducer reducer;

	public ReducingStage() throws Exception {
		super(KeyPayload.class, Dht.GetResp.class);
		ostore.util.TypeTable.register_type(KeyValue.class);
	}

	@Override
	public void init(ConfigDataIF config) throws Exception {
		super.init(config);
		String reducer_name = config_get_string(config, "reducer");
		reducer = (Reducer) classLoader.loadClass(reducer_name).newInstance();
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) {
			BambooRouteDeliver deliver = (BambooRouteDeliver) item;
			KeyPayload payload = (KeyPayload) deliver.payload;
			requestGet(payload.key);
		} else if (item instanceof Dht.GetResp) {
			Dht.GetResp resp = (GetResp) item;
			reducer.reduce((String) resp.user_data, parseGetResp(resp), this);
		} else {
			BUG("Unexpected event:" + item);
		}
	}

	@Override
	public long getAppID() {
		return app_id;
	}

	@Override
	public void collect(String key, String value) {
		KeyValue p = new KeyValue(key, value);
		dispatchTo(BigInteger.ZERO, MasterStage.app_id, p);
	}
}
