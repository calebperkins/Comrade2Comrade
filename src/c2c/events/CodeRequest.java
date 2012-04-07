package c2c.events;

import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.QSException;

/**
 * If a mapper does not have the code available to execute a job, it uses this
 * to send a network message directly to the master.
 * 
 * Usage:
 * 
 * 1) Mapper does not have code. Dispatches
 * CodeRequest(BambooRouteDeliver.immediate_src)
 * 
 * 2) Master receives CodeRequest. It has IP of Mapper though
 * CodeRequest.sender.
 * 
 * @author Caleb Perkins
 * 
 */
public class CodeRequest extends NetworkMessage {

	public CodeRequest(NodeId master) {
		super(master, false);
	}

	public CodeRequest(InputBuffer in) throws QSException {
		super(in);
	}

}
