package c2c.payloads;

import java.math.BigInteger;
import bamboo.util.GuidTools;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;
import ostore.util.SHA1Hash;

public class KeyPayload implements QuickSerializable, Comparable<KeyPayload> {
	public final String domain;
	public final String data;

	public KeyPayload(String domain, String key) {
		this.domain = domain;
		data = key;
	}

	public KeyPayload(InputBuffer buf) {
		domain = buf.nextString();
		data = buf.nextString();
	}
	
	public static KeyPayload intermediateKeys(String domain) {
		return new KeyPayload(domain, "c2c:intermediate");
	}
	
	public boolean isMeta() {
		return data.startsWith("c2c:");
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(domain);
		buf.add(data);
	}

	/**
	 * Returns a 20-bit key suitable for use in the Bamboo DHT.
	 * @return a DHT key
	 */
	public BigInteger toNode() {
		return GuidTools.secure_hash_to_big_integer(new SHA1Hash(this));
	}

	@Override
	public String toString() {
		return domain + "/" + data;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + data.hashCode();
		result = prime * result + domain.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof KeyPayload && compareTo((KeyPayload) obj) == 0;
	}

	@Override
	public int compareTo(KeyPayload other) {
		int d = domain.compareTo(other.domain);
		if (d == 0) {
			return data.compareTo(other.data);
		}
		return d;
	}

}
