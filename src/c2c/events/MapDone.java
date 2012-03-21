package c2c.events;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

public class MapDone implements QuickSerializable {
	public final String key;

	public MapDone(String k) {
		key = k;
	}

	public MapDone(InputBuffer b) throws QSException {
		key = b.nextString();
	}

	@Override
	public void serialize(OutputBuffer b) {
		b.add(key);
	}
}
