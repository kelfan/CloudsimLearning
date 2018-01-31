/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.merge;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.core.predicates.PredicateType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class represents an Aggregate Switch in a Datacenter network. 
 * It interacts with other switches in order to exchange packets.
 * 
 * <br/>Please refer to following publication for more details:<br/>
 * <ul>
 * <li><a href="http://dx.doi.org/10.1109/UCC.2011.24">Saurabh Kumar Garg and Rajkumar Buyya, NetworkCloudSim: Modelling Parallel Applications in Cloud
 * Simulations, Proceedings of the 4th IEEE/ACM International Conference on Utility and Cloud
 * Computing (UCC 2011, IEEE CS Press, USA), Melbourne, Australia, December 5-7, 2011.</a>
 * </ul>
 *  
 * @author Saurabh Kumar Garg
 * @since CloudSim Toolkit 1.0
 */
public class MergeAggregateSwitch extends MergeSwitch {

	/**
	 * Instantiates a Aggregate Switch specifying the switches that are connected to its
	 * downlink and uplink ports and corresponding bandwidths.
	 *
	 * @param name Name of the switch
	 * @param level At which level the switch is with respect to hosts.
	 * @param dc The Datacenter where the switch is connected to
	 */
	public MergeAggregateSwitch(String name, int level, MergeDatacenter dc) {
		super(name, level, dc);
		downlinkswitchpktlist = new HashMap<Integer, List<MergeNetworkPacket>>();
		uplinkswitchpktlist = new HashMap<Integer, List<MergeNetworkPacket>>();
		uplinkbandwidth = MergeConstants.BandWidthAggRoot;
		downlinkbandwidth = MergeConstants.BandWidthEdgeAgg;
		latency = MergeConstants.SwitchingDelayAgg;
		numport = MergeConstants.AggSwitchPort;
		uplinkswitches = new ArrayList<MergeSwitch>();
		downlinkswitches = new ArrayList<MergeSwitch>();
	}

	@Override
	protected void processpacket_down(SimEvent ev) {
		// packet coming from up level router.
		// has to send downward
		// check which switch to forward to
		// add packet in the switch list
		// add packet in the host list
		MergeNetworkPacket hspkt = (MergeNetworkPacket) ev.getData();
		int recvVMid = hspkt.pkt.reciever;
		CloudSim.cancelAll(getId(), new PredicateType(CloudSimTags.Network_Event_send));
		schedule(getId(), latency, CloudSimTags.Network_Event_send);

		if (level == MergeConstants.Agg_LEVEL) {
			// packet is coming from root so need to be sent to edgelevel swich
			// find the id for edgelevel switch
			int switchid = dc.VmToSwitchid.get(recvVMid);
			List<MergeNetworkPacket> pktlist = downlinkswitchpktlist.get(switchid);
			if (pktlist == null) {
				pktlist = new ArrayList<MergeNetworkPacket>();
				downlinkswitchpktlist.put(switchid, pktlist);
			}
			pktlist.add(hspkt);
			return;
		}

	}

	@Override
	protected void processpacket_up(SimEvent ev) {
		// packet coming from down level router.
		// has to send up
		// check which switch to forward to
		// add packet in the switch list
		//
		// int src=ev.getSource();
		MergeNetworkPacket hspkt = (MergeNetworkPacket) ev.getData();
		int recvVMid = hspkt.pkt.reciever;
		CloudSim.cancelAll(getId(), new PredicateType(CloudSimTags.Network_Event_send));
		schedule(getId(), switching_delay, CloudSimTags.Network_Event_send);

		if (level == MergeConstants.Agg_LEVEL) {
			// packet is coming from edge level router so need to be sent to
			// either root or another edge level swich
			// find the id for edgelevel switch
			int switchid = dc.VmToSwitchid.get(recvVMid);
			boolean flagtoswtich = false;
			for (MergeSwitch sw : downlinkswitches) {
				if (switchid == sw.getId()) {
					flagtoswtich = true;
				}
			}
			if (flagtoswtich) {
				List<MergeNetworkPacket> pktlist = downlinkswitchpktlist.get(switchid);
				if (pktlist == null) {
					pktlist = new ArrayList<MergeNetworkPacket>();
					downlinkswitchpktlist.put(switchid, pktlist);
				}
				pktlist.add(hspkt);
			} else// send to up
			{
				MergeSwitch sw = uplinkswitches.get(0);
				List<MergeNetworkPacket> pktlist = uplinkswitchpktlist.get(sw.getId());
				if (pktlist == null) {
					pktlist = new ArrayList<MergeNetworkPacket>();
					uplinkswitchpktlist.put(sw.getId(), pktlist);
				}
				pktlist.add(hspkt);
			}
		}
	}

}
