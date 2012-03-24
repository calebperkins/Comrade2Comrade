package c2c.payloads;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;

public class KeyPayload implements QuickSerializable {
	public final String key;

	public KeyPayload(String key) {
		this.key = key;
	}

	public KeyPayload(InputBuffer buf) {
		key = buf.nextString();
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(key);
	}

	@Override
	public String toString() {
		return key;
	}

}
