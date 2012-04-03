package c2c.payloads;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;

public class KeyPayload implements QuickSerializable {
	public final String domain;
	public final String key;

	public KeyPayload(String domain, String key) {
		this.domain = domain;
		this.key = key;
	}

	public KeyPayload(InputBuffer buf) {
		domain = buf.nextString();
		key = buf.nextString();
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(domain);
		buf.add(key);
	}

	@Override
	public String toString() {
		return domain + "/" + key;
	}

}
