package c2c.utilities;

import java.util.HashMap;
import java.util.Map;

public class ByteClassLoader extends ClassLoader {
	private Map<String, byte[]> klasses = new HashMap<String, byte[]>();
	
	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		byte[] c = klasses.get(name);
		if (c == null)
			throw new ClassNotFoundException();
		else
			return defineClass(name, c, 0, c.length);
	}
}
