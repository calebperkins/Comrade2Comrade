package c2c.events;

import seda.sandStorm.api.QueueElementIF;

public class MappingUnderway implements QueueElementIF {
	public final int expected;

	public MappingUnderway(int e) {
		expected = e;
	}
}
