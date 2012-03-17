package c2c.events;

import java.math.BigInteger;

import ostore.util.InputBuffer;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.QueueElementIF;

public class MapDone implements QuickSerializable, QueueElementIF {
	public final BigInteger node;

	public MapDone(BigInteger n) {
		node = n;
	}

	public MapDone(InputBuffer b) throws QSException {
		node = b.nextBigInteger();
	}

	@Override
	public void serialize(OutputBuffer b) {
		b.add(node);
	}
}
