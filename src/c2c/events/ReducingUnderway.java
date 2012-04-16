package c2c.events;

import seda.sandStorm.api.QueueElementIF;

public class ReducingUnderway implements QueueElementIF {
	public final String domain;
	public final int reducers;
	
	public ReducingUnderway(String domain, int reducers) {
		this.domain = domain;
		this.reducers = reducers;
	}
}
