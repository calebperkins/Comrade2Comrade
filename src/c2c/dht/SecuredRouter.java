package c2c.dht;

import java.util.Iterator;
import java.util.LinkedList;

import ostore.util.*;
import bamboo.router.*;
import bamboo.lss.*;
import bamboo.vivaldi.*;
import seda.sandStorm.api.*;

public class SecuredRouter extends Router {
    public String join_password, accept_password;

    public SecuredRouter() throws Exception {
    	super();
    }

    public void init (ConfigDataIF config) throws Exception {
    	super.init(config);
    	join_password = config_get_string(config, "join_password");
    	accept_password = config_get_string(config, "accept_password");
    	
    	joinAlarm = new bamboo.util.Curry.Thunk3<Integer,Integer,Integer>() {
            public void run(Integer tries, Integer period, Integer revTTL) {
                if (! initialized) {
                    tries = new Integer(tries.intValue() + 1);
                    revTTL = new Integer(revTTL.intValue() + 1);
                    period = new Integer(period.intValue() >= 30*1000 
                                         ? 60*1000 : period.intValue() * 2);
                    int divisor = Math.max (3, gateways.size ());
                    NodeId gateway = gateways.removeFirst ();
                    gateways.addLast (gateway);
                    logger.info ("Join try " + tries +
                            " timed out.  Gateway=" + gateway + ".  Trying again " +
                            " with rev_ttl=" + revTTL.intValue()/divisor);
                    dispatch (new SecureJoinReq (gateway, my_node_id, my_guid, 
                                           revTTL.intValue()/divisor, join_password));
                    acore.registerTimer(randomPeriod(period.intValue()),
                                        bamboo.util.Curry.curry(this, tries, period, revTTL));
                }
            }
        };
    	partitionCheckAlarm  = new Runnable() {
            public void run() {
                if (down_nodes.size () > 0) {
                    int which = rand.nextInt (down_nodes.size ());
                    Iterator<NodeId> i = down_nodes.iterator ();
                    NodeId n = null;
                    while (which-- >= 0)
                        n = i.next ();

                    SecureJoinReq outb = new SecureJoinReq (n, my_node_id, my_guid, 0, join_password);
                    outb.timeout_sec = 10;
                    outb.comp_q = my_sink;
                    outb.user_data = new PartitionCheckCB (n);
                    if (logger.isDebugEnabled ()) logger.debug (
                            "sending " + outb + " to check for partition");
                    dispatch (outb);
                }
                else {
                    if (logger.isDebugEnabled ()) logger.debug ("no down nodes");
                }

                if (partition_check_alarm_period != 0) {
                    acore.registerTimer(
                            randomPeriod(partition_check_alarm_period), this);
                }
            }
        };
        ready = new Runnable() {
            public void run() {
                network = Network.instance(my_node_id);
                vivaldi = Vivaldi.instance(my_node_id);
                rpc = Rpc.instance(my_node_id);
                try {
                    rpc.registerRequestHandler(CoordReq.class, coordReqHandler);
                }
                catch (DuplicateTypeException e) { BUG(e); }
                start_time_ms = now_ms ();
                if (gateways.isEmpty ()) {
                    logger.info ("Joined through gateway " + my_node_id);
                    set_initialized ();
                    notify_leaf_set_changed ();
                }
                else {
                    NodeId gateway = gateways.removeFirst ();
                    gateways.addLast (gateway);
                    logger.info ("Trying to join through gateway " + gateway);
                    dispatch (new SecureJoinReq (gateway, my_node_id, my_guid, 0, join_password));
                    acore.registerTimer(randomPeriod(10*1000), 
                            bamboo.util.Curry.curry(joinAlarm, new Integer(0), 
                                new Integer(10*1000), // 10 seconds for starters
                                new Integer(0)));
                } 
                acore.registerTimer(randomPeriod(periodic_ping_period), pingAlarm);
                acore.registerTimer(randomPeriod(COORD_CHECK), expireCoordsAlarm);
                acore.registerTimer(randomPeriod(send_coord_period), 
                                    sendCoordsAlarm);
            }
        };
    }

    protected void handle_ping_msg(PingMsg msg) {
    	//logger.info("ping!");
    }
    
    protected void handle_secure_join_req(SecureJoinReq req) {
    	if (req.password == accept_password) {
    		logger.info(req + " has correct password, processing join request");
    		
    		// Check for routing loops, and if one is found, just drop the
    		// message.  Either the network will heal, or the rev_ttl will be
    		// increased by the joining node until the loop is avoided.

    		if (req.path.contains (my_neighbor_info)) {
    			logger.warn ("loop in join path: " + req);
    			return;
    		}

    		NeighborInfo joiner = new NeighborInfo (req.node_id, req.guid);

    		// Don't use location cache for joins.
    		NeighborInfo next_hop = calc_next_hop (req.guid, false);
    		int hops_to_go = est_hops_to_go (req.guid, false);

    		// Don't add nodes to the location cache w/o direct
    		// confirmation that they're up, such as receiving a message
    		// from them.
    		// location_cache.add_node (joiner);

    		if ((hops_to_go == req.rev_ttl) ||
    				(next_hop == my_neighbor_info) ||
    				next_hop.node_id.equals (req.node_id)) {

    			if (next_hop.node_id.equals (req.node_id)
    					&& logger.isDebugEnabled ())
    				logger.debug ("next hop for " + req + " is joining node!");

    			// We're the root.  Send a response.

    			LinkedList<NeighborInfo> path = new LinkedList<NeighborInfo>();
    			for (NeighborInfo n : req.path) 
    				path.addLast(n);
    			path.addLast (my_neighbor_info);

    			dispatch (new JoinResp (req.node_id, path, leaf_set.as_set ()));

    			// Add it to our leaf set and routing table.

    			add_to_rt (joiner);
    			add_to_ls (joiner);
    		}
    		else {
    			final SecureJoinReq orig = req;
    			try {
    				req = (SecureJoinReq) req.clone ();
    			}
    			catch (CloneNotSupportedException e) {
    				BUG (e);
    			}
    			req.path.addLast (my_neighbor_info);
    			req.peer = next_hop.node_id;
    			req.inbound = false;
    			req.comp_q = my_sink;
    			req.user_data = new RecursiveRouteCB (next_hop, 
    					new Runnable() { public void run() { handleEvent(orig); }});
    			req.timeout_sec = 5;
    			dispatch (req);
    		}	
    	} else {
    		logger.info(req + " has wrong password (expecting: " + accept_password
    				+ ", got: " + req.password + "), ignoring"); 
    	}
    }
    
    public void handleEvent(QueueElementIF item) {
    	//logger.info("SecuredRouter.handleEvent()");
    	if (item instanceof PingMsg) {
    		handle_ping_msg((PingMsg) item);
    	} else if (item instanceof SecureJoinReq) {
    		handle_secure_join_req((SecureJoinReq) item);
    	} else {
    		super.handleEvent(item);
    	}
    }
}