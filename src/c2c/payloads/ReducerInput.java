package c2c.payloads;

import ostore.util.*;

public class ReducerInput implements QuickSerializable {
	public final String key;

	public ReducerInput(String key) {
		this.key = key;
	}

	public ReducerInput(InputBuffer buf) {
		key = buf.nextString();
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(key);
	}

}
