package c2c.payloads;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

public class ReducerOutput implements QuickSerializable {
	public final String value;

	public ReducerOutput(String v) {
		value = v;
	}

	public ReducerOutput(InputBuffer b) throws QSException {
		value = b.nextString();
	}

	@Override
	public void serialize(OutputBuffer b) {
		b.add(value);
	}

	@Override
	public String toString() {
		return value;
	}

}
