package c2c.payloads;

import java.math.BigInteger;

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
public class MapPayload implements QuickSerializable {
	public final BigInteger job_guid;
	public final String key;
	public final String value;

	public MapPayload(BigInteger job_id, String k, String v) {
		job_guid = job_id;
		key = k;
		value = v;
	}

	public MapPayload(InputBuffer buf) throws QSException {
		job_guid = buf.nextBigInteger();
		key = buf.nextString();
		value = buf.nextString();
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(job_guid);
		buf.add(key);
		buf.add(value);
	}

	@Override
	public int hashCode() {
		return key.hashCode() ^ value.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof MapPayload) {
			MapPayload other = (MapPayload) obj;
			return other.key.equals(key) && other.value.equals(value);
		}
		return false;
	}

	@Override
	public String toString() {
		return key + ": " + value;
	}

}