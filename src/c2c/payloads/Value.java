package c2c.payloads;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Random;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

public class Value implements QuickSerializable {
	public final String value;
	private final int version;
	private static final Random rand = new Random();
	private static final String DELIMITER = ":";
	public static final Charset CHARSET = Charset.forName("UTF-8");
	private static final CharsetDecoder decoder = CHARSET.newDecoder();

	public Value(String value, boolean fresh) {
		this.value = value;
		version = fresh ? rand.nextInt() : 0;
	}

	public Value(InputBuffer in) throws QSException {
		value = in.nextString();
		version = in.nextInt();
	}

	public Value(ByteBuffer in) throws CharacterCodingException {
		String[] raw = decoder.decode(in).toString().split(DELIMITER, 2);
		version = Integer.parseInt(raw[0]);
		value = raw[1];
	}

	@Override
	public void serialize(OutputBuffer out) {
		out.add(value);
		out.add(version);
	}

	public byte[] hash() {
		return BigInteger.valueOf(version ^ value.hashCode()).toByteArray();
	}

	public ByteBuffer toByteBuffer() {
		String s = version + DELIMITER + value;
		return ByteBuffer.wrap(s.getBytes(CHARSET));
	}

}
