package c2c.utilities;

import java.math.BigInteger;

import c2c.api.MapReduceApplication;
import c2c.api.Mapper;
import c2c.api.Reducer;

public class RemoteJob {
	private final BigInteger master;
	protected final MapReduceApplication app;
	
	private static final ClassLoader classLoader = RemoteJob.class.getClassLoader();

	public RemoteJob(String domain, BigInteger master) throws Exception {
		String klass = domain.split(":", 2)[0];
		app = (MapReduceApplication) classLoader.loadClass(klass).newInstance();
		this.master = master;
	}
	
	public Mapper getMapper() {
		return app;
	}
	
	public Reducer getReducer() {
		return app;
	}
	
	public BigInteger getMaster() {
		return master;
	}
	
}
