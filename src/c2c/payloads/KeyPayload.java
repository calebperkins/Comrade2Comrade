package c2c.payloads;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QuickSerializable;

public class KeyPayload implements QuickSerializable, Comparable<KeyPayload> {
	public final String domain;
	public final String key;

	public KeyPayload(String domain, String key) {
		this.domain = domain;
		this.key = key;
	}

	public KeyPayload(InputBuffer buf) {
		domain = buf.nextString();
		key = buf.nextString();
	}

	@Override
	public void serialize(OutputBuffer buf) {
		buf.add(domain);
		buf.add(key);
	}

	public BigInteger toNode() {
		try {
			MessageDigest cript = MessageDigest.getInstance("SHA-1");
			cript.reset();
			cript.update((domain + key).getBytes(Value.CHARSET));
			return new BigInteger(cript.digest());
		} catch (NoSuchAlgorithmException e) {
			return BigInteger.ZERO;
		}
	}

	@Override
	public String toString() {
		return domain + "/" + key;
	}

	@Override
	public int compareTo(KeyPayload other) {
		int d = domain.compareTo(other.domain);
		if (d == 0) {
			return key.compareTo(other.key);
		}
		return d;
	}

}
