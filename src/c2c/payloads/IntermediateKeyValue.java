package c2c.payloads;

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
