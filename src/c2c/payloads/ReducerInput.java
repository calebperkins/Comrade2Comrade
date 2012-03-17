package c2c.payloads;

import java.util.ArrayList;
import java.util.List;

import ostore.util.*;

public class ReducerInput implements QuickSerializable {
	public final String key;
	public final List<String> values;

	public ReducerInput(String key, List<String> values) {
		this.key = key;
		this.values = values;
	}

	public ReducerInput(InputBuffer buf) {
		key = buf.nextString();
		int size = buf.nextInt();
		values = new ArrayList<String>(size);
		for (int i = 0; i < size; i++) {
			values.add(buf.nextString());
		}
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(key);
		buf.add(values.size());
		for (String value : values) {
			buf.add(value);
		}
	}

}
