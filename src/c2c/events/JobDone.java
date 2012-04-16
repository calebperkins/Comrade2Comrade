package c2c.events;

import seda.sandStorm.api.QueueElementIF;

public class JobDone implements QueueElementIF {
	public final String domain;
	
	public JobDone(String d) {
		domain = d;
	}
}
