package c2c.application;

import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;

import c2c.utilities.*;

public class RemoteJobRunner implements Remote {
	public RemoteJobRunner() {
		try {
			if (System.getSecurityManager() == null) {
				System.setSecurityManager(new SecurityManager());
			}
			RemoteJobRunner stub = (RemoteJobRunner) UnicastRemoteObject.exportObject(this, 0);
			Registry registry = LocateRegistry.getRegistry();
			registry.rebind(Constants.REMOTE_COMPUTE, stub);
			// Ready to accept jobs
		} catch (Exception e) {
			// Failed to bind RMI
		}
	}
	
	public <T> T executeTask(Task<T> t) {
		return t.execute();
	}
}
