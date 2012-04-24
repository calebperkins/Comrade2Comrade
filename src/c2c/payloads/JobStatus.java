package c2c.payloads;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

public class JobStatus implements QuickSerializable {
	public final String domain;
	public final boolean done, mapper;
	
	public JobStatus(String domain, boolean done, boolean mapper) {
		this.domain = domain;
		this.done = done; this.mapper = mapper;
	}
	
	public JobStatus(InputBuffer in) throws QSException {
		domain = in.nextString();
		done = in.nextBoolean(); mapper = in.nextBoolean();
	}
	
	@Override
	public void serialize(OutputBuffer out) {
		out.add(domain);
		out.add(done); out.add(mapper);
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Job key=");
		sb.append(domain);
		sb.append(" done=");
		sb.append(done);
		sb.append(" mapper=");
		sb.append(mapper);
		return sb.toString();
	}

}
