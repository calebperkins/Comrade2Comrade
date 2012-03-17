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
public class MapPair implements QuickSerializable, Comparable<MapPair> {
	public final String key;
	public final String value;

	public MapPair(String k, String v) {
		key = k;
		value = v;
	}

	public MapPair(InputBuffer buf) throws QSException {
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
		if (obj instanceof MapPair) {
			MapPair other = (MapPair) obj;
			return other.key.equals(key) && other.value.equals(value);
		}
		return false;
	}

	@Override
	public String toString() {
		return key + ": " + value;
	}

	@Override
	public int compareTo(MapPair x) {
		return key.compareTo(x.key);
	}

}