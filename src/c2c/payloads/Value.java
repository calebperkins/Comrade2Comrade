package c2c.payloads;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.Random;

public class Value {
	public final String value;
	private final int version;
	private static final Random rand = new Random();
	private static final String DELIMITER = ":";
	public static final Charset CHARSET = Charset.forName("UTF-8");

	public Value(String value, boolean fresh) {
		this.value = value;
		version = fresh ? rand.nextInt() : 0;
	}

	public Value(ByteBuffer in) throws CharacterCodingException {
		String[] raw = CHARSET.newDecoder().decode(in).toString().split(DELIMITER, 2);
		version = Integer.parseInt(raw[0]);
		value = raw[1];
	}

	public byte[] hash() {
		return BigInteger.valueOf(version ^ value.hashCode()).toByteArray();
	}

	public ByteBuffer toByteBuffer() {
		String s = version + DELIMITER + value;
		return ByteBuffer.wrap(s.getBytes(CHARSET));
	}

	@Override
	public String toString() {
		return value;
	}

}
