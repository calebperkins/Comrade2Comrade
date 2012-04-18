package c2c.payloads;

import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;

public class IntermediateKeyValue {
	public final KeyPayload key;
	public final Value value;
	public final KeyPayload creator;
	
	public IntermediateKeyValue(KeyPayload creator, KeyPayload k, Value v) {
		this.creator = creator;
		key = k;
		value = v;
	}

}
