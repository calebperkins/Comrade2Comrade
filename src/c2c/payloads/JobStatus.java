package c2c.payloads;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

public class JobStatus implements QuickSerializable {
	public final KeyPayload key;
	public final boolean done, mapper;
	
	public JobStatus(KeyPayload key, boolean done, boolean mapper) {
		this.key = key;
		this.done = done;
		this.mapper = mapper;
	}
	
	public JobStatus(InputBuffer in) throws QSException {
		key = (KeyPayload) in.nextObject();
		done = in.nextBoolean();
		mapper = in.nextBoolean();
	}
	
	@Override
	public void serialize(OutputBuffer out) {
		out.add(key);
		out.add(done);
		out.add(mapper);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Job key=");
		sb.append(key);
		sb.append(" done=");
		sb.append(done);
		sb.append(" mapper=");
		sb.append(mapper);
		return sb.toString();
	}

}
