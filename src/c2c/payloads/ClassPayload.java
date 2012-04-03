package c2c.payloads;

import c2c.utilities.ByteClassLoader;
import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;

public class ClassPayload implements QuickSerializable {
	public final String name;
	private final int len;
	public final byte[] klass;
	
	private static ByteClassLoader classLoader = new ByteClassLoader();
	
	public ClassPayload(InputBuffer in) throws QSException {
		name = in.nextString();
		len = in.nextInt();
		klass = new byte[len];
		in.nextBytes(klass, 0, len);
	}

	@Override
	public void serialize(OutputBuffer out) {
		out.add(name);
		out.add(len);
		out.add(klass);
	}
	
	public Class<?> toClass() {
		try {
			return classLoader.loadClass(name);
		} catch (ClassNotFoundException e) {
			return null;
		}
	}

}
