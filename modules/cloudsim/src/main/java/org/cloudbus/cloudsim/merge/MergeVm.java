/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.merge;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.network.datacenter.NetworkCloudlet;

import java.util.ArrayList;

/**
 * NetworkVm class extends {@link Vm} to support simulation of networked datacenters. 
 * It executes actions related to management of packets (sent and received).
 * 
 * <br/>Please refer to following publication for more details:<br/>
 * <ul>
 * <li><a href="http://dx.doi.org/10.1109/UCC.2011.24">Saurabh Kumar Garg and Rajkumar Buyya, NetworkCloudSim: Modelling Parallel Applications in Cloud
 * Simulations, Proceedings of the 4th IEEE/ACM International Conference on Utility and Cloud
 * Computing (UCC 2011, IEEE CS Press, USA), Melbourne, Australia, December 5-7, 2011.</a>
 * </ul>
 * 
 * @author Saurabh Kumar Garg
 * @since CloudSim Toolkit 3.0
 * @todo Attributes should be private
 */
public class MergeVm extends Vm implements Comparable<Object> {
        /**
         * List of {@link MergeCloudlet} of the VM.
         */
	public ArrayList<MergeCloudlet> cloudletlist;

        /**
         * @todo It doesn't appear to be used.
         */
	int type;

        /**
         * List of packets received by the VM.
         */
	public ArrayList<MergeHostPacket> recvPktlist;

        /**
         * @todo It doesn't appear to be used.
         */
	public double memory;

        /**
         * @todo It doesn't appear to be used.
         */
	public boolean flagfree;

        /**
         * The time when the VM finished to process its cloudlets.
         */
	public double finishtime;

	public MergeVm(
			int id,
			int userId,
			double mips,
			int pesNumber,
			int ram,
			long bw,
			long size,
			String vmm,
			CloudletScheduler cloudletScheduler) {
		super(id, userId, mips, pesNumber, ram, bw, size, vmm, cloudletScheduler);

		cloudletlist = new ArrayList<MergeCloudlet>();
	}

	public boolean isFree() {
		return flagfree;
	}

	@Override
	public int compareTo(Object arg0) {
		MergeVm hs = (MergeVm) arg0;
		if (hs.finishtime > finishtime) {
			return -1;
		}
		if (hs.finishtime < finishtime) {
			return 1;
		}
		return 0;
	}
}
