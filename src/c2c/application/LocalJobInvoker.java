package c2c.application;

import java.rmi.*;
import java.rmi.registry.*;

import c2c.utilities.Constants;

public class LocalJobInvoker {
	public LocalJobInvoker() {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
	}
	
	public static <T> T executeRemote(Task<T> t, String remoteHost) {
		T result = null;
		
		try {
			Registry registry = LocateRegistry.getRegistry(remoteHost);
			RemoteJobRunner comp = (RemoteJobRunner) registry.lookup(Constants.REMOTE_COMPUTE);
			result = comp.executeTask(t);
		} catch (Exception e) {
			// Remote execution exception
		}
		return result;
	}
}
