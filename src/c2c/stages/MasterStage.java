package c2c.stages;

import java.math.BigInteger;
import java.util.Random;

import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;

import c2c.events.*;
import c2c.payloads.*;

import bamboo.api.*;

/**
 * Takes job requests from a Client and disperses them to mappers.
 * 
 * @author caleb
 * 
 */
public final class MasterStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(MasterStage.class);
	private final Random rand;
	private boolean test_mode;

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		bamboo.lss.DustDevil.main(args);
	}

	public MasterStage() throws Exception {
		super(MapPair.class, JobRequest.class);
		rand = new Random();
	}

	@Override
	public void init(ConfigDataIF config) throws Exception {
		super.init(config);
		String mode = config_get_string(config, "mode");
		test_mode = mode != null && mode.equals("master");
		if (test_mode)
			sendTestJob();
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) { // get back the results
			BambooRouteDeliver deliver = (BambooRouteDeliver) item;
			logger.info("Results back: " + deliver.payload);
		} else if (item instanceof JobRequest) { // Distribute jobs.
			JobRequest req = (JobRequest) item;
			dispatch(new MappingUnderway(req.pairs.size()));
			for (MapPair pair : req.pairs) {
				// Distribute randomly. TODO: better algorithm
				BigInteger dest = bamboo.util.GuidTools.random_guid(rand);
				BambooRouteInit init = new BambooRouteInit(dest,
						MappingStage.app_id, false, false, pair);
				dispatch(init);
			}
		} else {
			BUG("Event " + item + " unknown.");
		}
	}

	/**
	 * Word count request.
	 */
	private void sendTestJob() {
		JobRequest req = new JobRequest();
		req.add("bears.txt", "Bears enjoy mauling and walks in the park");
		req.add("cats.txt", "Cats enjoy the park and they like cat nip too");
		req.add("dogs.txt", "Dogs enjoy walks and being silly");
		classifier.dispatch_later(req, 5000);
	}

	@Override
	public long getAppID() {
		return app_id;
	}

}
