package c2c.stages;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import c2c.api.*;
import c2c.events.JobDone;
import c2c.events.MappingUnderway;
import c2c.payloads.IntermediateKeyValue;
import c2c.payloads.KeyPayload;
import c2c.payloads.KeyValue;
import c2c.payloads.Value;
import c2c.utilities.WorkerTable;

import seda.sandStorm.api.*;
import bamboo.api.*;
import bamboo.dht.Dht;
import bamboo.dht.Dht.PutResp;
import bamboo.dht.bamboo_stat;
import bamboo.router.PingMsg;

public final class MappingStage extends MapReduceStage {
	private final ClassLoader classLoader = MappingStage.class.getClassLoader();

	// Here KeyPayload corresponds to a mapper key
	private final Map<KeyPayload, Integer> remaining = new HashMap<KeyPayload, Integer>();
	
	private final Map<String, Job> jobs = new HashMap<String, MappingStage.Job>();
	private boolean working; // Job active?
	
	private class Job {
		public final BigInteger master;
		public final Mapper mapper;
		
		public Job(String domain, BigInteger master) throws Exception {
			mapper = (Mapper) classLoader.loadClass(domain).newInstance();
			this.master = master;
		}
	}

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

	private Job getJob(String domain, BigInteger master) {
		if (!jobs.containsKey(domain)) {
			try {
				jobs.put(domain, new Job(domain, master));
			} catch (Exception e) {
				BUG(e);
			}
		}
		return jobs.get(domain);
	}

	private void handleMapRequest(BambooRouteDeliver event) {
		final KeyValue kv = (KeyValue) event.payload;
		Job job = getJob(kv.key.domain, event.src);
		
		// Notify the master that we're now working
		working = true;
		acore.register_timer(10, new Runnable() {
			public void run() {
				if (working) {
					dispatch(new MappingUnderway(kv.key.domain, -1));
					acore.register_timer(1000, this);
				}
			}
		});
		
		logger.info("Mapping " + kv.key);
		Collector c = new Collector(kv.key);
		job.mapper.map(kv.key.data, kv.value, c);
		c.flush();
		working = false;
		
		// Tell the master - we're done
		dispatch(new JobDone(kv.key.domain));
	}

	private void handlePutResp(Dht.PutResp response) {
		IntermediateKeyValue kv = (IntermediateKeyValue) response.user_data;
		if (response.result == bamboo_stat.BAMBOO_OK) {
			remaining.put(kv.creator, remaining.get(kv.creator) - 1);
			if (remaining.get(kv.creator) == 0) {
				dispatchTo(jobs.get(kv.key.domain).master, PartitioningStage.app_id, kv.creator);
			}
		} else {
			logger.debug("Repeating put...");
			doPut(kv);
		}
	}
	
	private void doPut(IntermediateKeyValue kv) {
		Dht.PutReq req = new Dht.PutReq(kv.key.toNode(), kv.value.toByteBuffer(),
				kv.value.hash(), true, my_sink, kv, 600,
				my_node_id.address());
		classifier.dispatch_later(req, 5000);
	}

	@Override
	public long getAppID() {
		return app_id;
	}

	private class Collector implements OutputCollector {
		private KeyPayload mapping_key;
		
		private Set<String> keys = new HashSet<String>();
		private Collection<KeyValue> keyvalues = new LinkedList<KeyValue>();

		public Collector(KeyPayload mapping_key) {
			this.mapping_key = mapping_key;
			assert mapping_key != null;
		}
		
		public void flush() {
			remaining.put(mapping_key, keys.size() + keyvalues.size());
			KeyPayload inter = intermediateKeys(mapping_key.domain);
			for (String key : keys) {
				makePut(inter, key, false);
			}
			for (KeyValue kv : keyvalues) {
				makePut(kv.key, kv.value, true);
			}
		}

		@Override
		public void collect(String key, String value) {
			keys.add(key);
			keyvalues.add(new KeyValue(new KeyPayload(mapping_key.domain, key), value));
		}
		
		private void makePut(KeyPayload key, String value, boolean allow_duplicates) {
			Value val = new Value(value, allow_duplicates);
			IntermediateKeyValue ud = new IntermediateKeyValue(mapping_key, key, val);
			Dht.PutReq req = new Dht.PutReq(key.toNode(), val.toByteBuffer(),
					val.hash(), true, my_sink, ud, 600,
					my_node_id.address());
			dispatch(req);
		}
	}

}
