package c2c.events;

import seda.sandStorm.api.QueueElementIF;

public class MappingUnderway implements QueueElementIF {
	public final String domain;
	public final int expected;

	public MappingUnderway(String domain, int e) {
		this.domain = domain;
		expected = e;
	}
}
