package c2c.stages;

import seda.sandStorm.api.*;
import bamboo.api.*;
import java.math.BigInteger;
import java.util.*;

import c2c.payloads.ReducerOutput;
import c2c.payloads.ReducerInput;

public class ReducingStage extends MapReduceStage {	
	public static final long app_id = bamboo.router.Router
			.app_id(ReducingStage.class);
	
	public ReducingStage() throws Exception {
		super(ReducerInput.class);
		ostore.util.TypeTable.register_type(ReducerOutput.class);
	}
	
	private String reduce(String key, List<String> values) {
		return key + ": " + values.size();
	}
	
	private void signalMaster(String result) {
		ReducerOutput p = new ReducerOutput(result);
		dispatch(new BambooRouteInit(BigInteger.ZERO, MasterStage.app_id, false, false, p));
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) {
			BambooRouteDeliver deliver = (BambooRouteDeliver) item;
			ReducerInput payload = (ReducerInput) deliver.payload;
			String v3 = reduce(payload.key, payload.values);
			
			signalMaster(v3);
		} else {
			BUG("Unexpected event:" + item);
		}
	}

	@Override
	public long getAppID() {
		return app_id;
	}
}
