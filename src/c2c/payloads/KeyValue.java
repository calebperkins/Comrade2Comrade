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
	public final String domain;

	public KeyValue(String domain, String key, String val) {
		this.key = key;
		value = val;
		this.domain = domain;
	}

	public KeyValue(InputBuffer buf) throws QSException {
		domain = buf.nextString();
		key = buf.nextString();
		value = buf.nextString();
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(domain);
		buf.add(key);
		buf.add(value);
	}

	@Override
	public int hashCode() {
		return key.hashCode() ^ value.hashCode() ^ domain.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof KeyValue) {
			KeyValue other = (KeyValue) obj;
			return other.key.equals(key) && other.value.equals(value) && other.domain.equals(domain);
		}
		return false;
	}

	@Override
	public String toString() {
		return domain + "/" + key + ": " + value;
	}

	@Override
	public int compareTo(KeyValue x) {
		return key.compareTo(x.key);
	}

}