package c2c.payloads;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

public class JobStatus implements QuickSerializable {
	public final KeyPayload key;
	public final boolean mapper;
	public final int status;
	
	
	public static final int STARTED = 0;
	public static final int FINISHED = 1;
	public static final int PERSISTED = 2;
	
	public JobStatus(KeyPayload key, int status, boolean mapper) {
		this.key = key;
		this.status = status;
		this.mapper = mapper;
	}
	
	public boolean isPersisted() {
		return status == PERSISTED;
	}
	
	public boolean isFinished() {
		return status == FINISHED;
	}
	
	public boolean isWorking() {
		return status == STARTED;
	}
	
	public JobStatus(InputBuffer in) throws QSException {
		key = (KeyPayload) in.nextObject();
		status = in.nextInt();
		mapper = in.nextBoolean();
	}
	
	@Override
	public void serialize(OutputBuffer out) {
		out.add(key);
		out.add(status);
		out.add(mapper);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Job key=");
		sb.append(key);
		sb.append(" done=");
		sb.append(status);
		sb.append(" mapper=");
		sb.append(mapper);
		return sb.toString();
	}

}
