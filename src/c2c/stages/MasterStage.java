package c2c.stages;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import ostore.util.TypeTable.DuplicateTypeCode;
import ostore.util.TypeTable.NotQuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.StagesInitializedSignal;

import c2c.events.MapRequest;
import c2c.payloads.MapPayload;

import bamboo.util.*;
import bamboo.api.*;

/**
 * Takes job requests from a Client and disperses them to mappers.
 * @author caleb
 *
 */
public final class MasterStage extends StandardStage {
	public static final long app_id = bamboo.router.Router
			.app_id(MasterStage.class);
	private final Random rand;
	private boolean test_mode;
	private boolean initialized = false;
	private final Queue<QueueElementIF> pending_events = new LinkedList<QueueElementIF>();
	private final HashSet<BigInteger> master_jobs = new HashSet<BigInteger>();
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		bamboo.lss.DustDevil.main(args);
	}
	
	public MasterStage() throws DuplicateTypeCode, NoSuchMethodException, NotQuickSerializable {
		super();
		rand = new Random();
		ostore.util.TypeTable.register_type(MapPayload.class);
		event_types = new Class[] { StagesInitializedSignal.class,
				MapRequest.class, BambooRouteDeliver.class };
	}
	
	@Override
	public void init(ConfigDataIF config) throws Exception {
		super.init(config);
		String mode = config_get_string(config, "mode");
		test_mode = mode != null && mode.equals("master");
	}

	@Override
	public void handleEvent(QueueElementIF item) {
		if (initialized)
			handleOperationalEvent(item);
		else
			handleInitializationEvent(item);
	}
	
	private void handleInitializationEvent(QueueElementIF item) {
		if (item instanceof StagesInitializedSignal) {
			dispatch(new BambooRouterAppRegReq(app_id, false, false, false,
					my_sink));
		} else if (item instanceof BambooRouterAppRegResp) {
			initialized = true;
			while (!pending_events.isEmpty())
				handleOperationalEvent(pending_events.remove());

			if (test_mode) // for debugging
				sendTestJob();
		} else {
			pending_events.add(item);
		}
	}
	
	private void handleOperationalEvent(QueueElementIF item) {
		if (item instanceof BambooRouteDeliver) { // get back the results
			BambooRouteDeliver deliver = (BambooRouteDeliver) item;
			logger.info("Results back: " + deliver.payload);
		} else if (item instanceof MapRequest) { // Distribute jobs.
			MapRequest req = (MapRequest) item;
			for (MapPayload pair : req.pairs) {
				// Distribute randomly. TODO: better algorithm
				BigInteger dest = bamboo.util.GuidTools.random_guid(rand);
				BambooRouteInit init = new BambooRouteInit(dest, MappingStage.app_id, false,
						false, pair);
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
		MapRequest req = new MapRequest(rand);
		req.add("bears.txt", "Bears enjoy mauling and walks in the park");
		req.add("cats.txt", "Cats enjoy the park and they like cat nip too");
		req.add("dogs.txt", "Dogs enjoy walks and being silly");
		
		master_jobs.add(req.guid);

		classifier.dispatch_later(req, 5000);
	}
	
	
}
