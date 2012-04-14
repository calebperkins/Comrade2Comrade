package c2c.events;

import seda.sandStorm.api.QueueElementIF;

public class MappingUnderway implements QueueElementIF {
	public final String domain;
	
	/**
	 * Expected number of mappers to hear back from
	 */
	public final int expected;

	public MappingUnderway(String domain, int e) {
		this.domain = domain;
		expected = e;
	}
}
