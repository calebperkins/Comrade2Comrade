package c2c.dht;

import java.math.BigInteger;
import bamboo.router.*;
import ostore.util.*;

public class SecureJoinReq extends JoinReq {
	public String password;
	
	public SecureJoinReq(NodeId dest, NodeId n, BigInteger g, int r, String pw) {
		super(dest, n, g, r);
		password = pw;
	}

	public SecureJoinReq(InputBuffer buffer) throws QSException {
		super(buffer);
		password = buffer.nextString();
	}
	
	public void serialize(OutputBuffer buffer) {
		super.serialize(buffer);
		buffer.add(password);
	}
	
	public Object clone() throws CloneNotSupportedException {
		SecureJoinReq result = (SecureJoinReq) super.clone();
		result.password = password;
		return result;
	}
	
	public String toString() {
		StringBuffer result = new StringBuffer(super.toString());
		result.append(" password=");
		result.append(password);
		return result.toString();
	}
}
