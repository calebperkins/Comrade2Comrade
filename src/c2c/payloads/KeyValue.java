package c2c.payloads;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.QueueElementIF;

/**
 * A key-value pair for mapping.
 * 
 * @author Caleb Perkins
 * 
 */
public class KeyValue implements QuickSerializable, Comparable<KeyValue>, QueueElementIF {
	public final KeyPayload key;
	public final String value;

	public KeyValue(String domain, String key, String val) {
		this.key = new KeyPayload(domain, key);
		value = val;
	}
	
	public KeyValue(KeyPayload key, String value) {
		this.key = key;
		this.value = value;
	}

	public KeyValue(InputBuffer buf) throws QSException {
		key = (KeyPayload) buf.nextObject();
		value = buf.nextString();
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(key);
		buf.add(value);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + key.hashCode();
		result = prime * result + value.hashCode();
		return result;
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