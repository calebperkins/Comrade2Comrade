package c2c.events;

import seda.sandStorm.api.QueueElementIF;

public class MappingFinished implements QueueElementIF {
	public final String domain;
	
	public MappingFinished(String domain) {
		this.domain = domain;
	}
}
