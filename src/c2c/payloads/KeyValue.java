package c2c.payloads;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

/**
 * A key-value pair for mapping.
 * 
 * @author caleb
 * 
 */
public class KeyValue implements QuickSerializable, Comparable<KeyValue> {
	public final String key;
	public final String value;

	public KeyValue(String k, String v) {
		key = k;
		value = v;
	}

	public KeyValue(InputBuffer buf) throws QSException {
		key = buf.nextString();
		value = buf.nextString();
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(key);
		buf.add(value);
	}

	@Override
	public int hashCode() {
		return key.hashCode() ^ value.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof KeyValue) {
			KeyValue other = (KeyValue) obj;
			return other.key.equals(key) && other.value.equals(value);
		}
		return false;
	}

	@Override
	public String toString() {
		return key + ": " + value;
	}

	@Override
	public int compareTo(KeyValue x) {
		return key.compareTo(x.key);
	}

}