package c2c.dht;

import java.net.InetAddress;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import bamboo.dht.Dht;

/**
 * One nasty hack to help unsuccessful puts.
 * 
 * We do not need Bamboo's fairness system. This speeds things up a bit.
 *
 */
public class FastDHT extends Dht {

	public FastDHT() throws Exception {
		super();
		Logger.getLogger(FastDHT.class).setLevel(Level.WARN);
	}

	@Override
	protected boolean below_fair_share(InetAddress arg0, int arg1, int arg2) {
		return true;
	}

	@Override
	protected boolean disk_space_avail(int arg0, int arg1) {
		return true;
	}

}
