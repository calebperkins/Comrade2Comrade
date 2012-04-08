package c2c.stages;

import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;

import c2c.events.*;
import c2c.payloads.*;

import bamboo.api.*;

/**
 * Takes job requests from a Client and disperses them to mappers.
 * 
 * @author Caleb Perkins
 * 
 */
public final class MasterStage extends MapReduceStage {
	public static final long app_id = bamboo.router.Router
			.app_id(MasterStage.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		bamboo.lss.DustDevil.main(args);
	}

	public MasterStage() throws Exception {
		super(KeyValue.class, JobRequest.class, CodeRequest.class);
	}

	@Override
	public void init(ConfigDataIF config) throws Exception {
		super.init(config);
		if (config_get_boolean(config, "master"))
			sendTestJob();
	}

	@Override
	protected void handleOperationalEvent(QueueElementIF event) {
		if (event instanceof BambooRouteDeliver) { // get back the results
			BambooRouteDeliver deliver = (BambooRouteDeliver) event;
			logger.info("Results back: " + deliver.payload);
		} else if (event instanceof JobRequest) { // Distribute jobs.
			JobRequest req = (JobRequest) event;
			dispatch(new MappingUnderway(req.domain, req.pairs.size()));
			for (KeyValue pair : req.pairs) {
				// Distribute randomly. TODO: better algorithm
				dispatchTo(randomNode(), MappingStage.app_id,
						pair);
			}
		} else if (event instanceof CodeRequest) {
			// TODO
		} else {
			BUG("Event " + event + " unknown.");
		}
	}

	/**
	 * Word count request.
	 */
	private void sendTestJob() { // TODO: remove
		JobRequest req = new JobRequest("demos.WordCount");
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
