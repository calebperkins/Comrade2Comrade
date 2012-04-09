package c2c.payloads;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;

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

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(domain);
		buf.add(data);
	}

	public BigInteger toNode() {
		try {
			MessageDigest cript = MessageDigest.getInstance("SHA-1");
			cript.reset();
			cript.update((domain + data).getBytes(Value.CHARSET));
			return new BigInteger(cript.digest());
		} catch (NoSuchAlgorithmException e) {
			return BigInteger.ZERO;
		}
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
