package c2c.payloads;

import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;

public class ReducerDone implements QuickSerializable {
	public final KeyPayload key;
	
	public ReducerDone(KeyPayload key) {
		this.key = key;
	}

	@Override
	public void serialize(OutputBuffer out) {
		// TODO Auto-generated method stub

	}

}
