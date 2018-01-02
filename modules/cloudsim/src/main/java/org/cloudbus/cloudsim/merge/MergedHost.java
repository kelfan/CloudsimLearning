/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.merge;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.HostStateHistoryEntry;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmScheduler;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.PeList;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsim.network.datacenter.*;
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.provisioners.BwProvisioner;
import org.cloudbus.cloudsim.provisioners.RamProvisioner;

/**
 * MergedHost class enables simulation of power-aware hosts.
 * 
 * <br/>If you are using any algorithms, policies or workload included in the power package please cite
 * the following paper:<br/>
 * 
 * <ul>
 * <li><a href="http://dx.doi.org/10.1002/cpe.1867">Anton Beloglazov, and Rajkumar Buyya, "Optimal Online Deterministic Algorithms and Adaptive
 * Heuristics for Energy and Performance Efficient Dynamic Consolidation of Virtual Machines in
 * Cloud Data Centers", Concurrency and Computation: Practice and Experience (CCPE), Volume 24,
 * Issue 13, Pages: 1397-1420, John Wiley & Sons, Ltd, New York, USA, 2012</a>
 * </ul>
 * 
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 2.0
 */
public class MergedHost extends Host {

	/** The power model used by the host. */
	private PowerModel powerModel;

	/**
	 * HostDynamicWorkload attributes
	 */
	/** The utilization mips. */
	private double utilizationMips;

	/** The previous utilization mips. */
	private double previousUtilizationMips;

	/** The host utilization state history. */
	private final List<HostStateHistoryEntry> stateHistory = new LinkedList<HostStateHistoryEntry>();

	/**
	 * Network attributes
	 */
	public List<NetworkPacket> packetTosendLocal;

	public List<NetworkPacket> packetTosendGlobal;
	/**
	 * List of received packets.
	 */
	public List<NetworkPacket> packetrecieved;
	/**
	 * @todo the attribute is not being used and is redundant with the ram capacity
	 *       defined in {@link Host#ramProvisioner}
	 */
	public double memory;

	/**
	 * Edge switch in which the Host is connected.
	 */
	public Switch sw;

	/**
	 * @todo What exactly is this bandwidth? Because it is redundant with the bw
	 *       capacity defined in {@link Host#bwProvisioner}
	 */
	public double bandwidth;

	/**
	 * Time when last job will finish on CPU1.
	 *
	 * @todo it is not being used.
	 **/
	public List<Double> CPUfinTimeCPU = new ArrayList<Double>();

	/**
	 * @todo it is not being used.
	 **/
	public double fintime = 0;


	/**
	 * Instantiates a new MergedHost.
	 * 
	 * @param id the id of the host
	 * @param ramProvisioner the ram provisioner
	 * @param bwProvisioner the bw provisioner
	 * @param storage the storage capacity
	 * @param peList the host's PEs list
	 * @param vmScheduler the VM scheduler
	 */
	public MergedHost(
			int id,
			RamProvisioner ramProvisioner,
			BwProvisioner bwProvisioner,
			long storage,
			List<? extends Pe> peList,
			VmScheduler vmScheduler,
			PowerModel powerModel) {
		super(id, ramProvisioner, bwProvisioner, storage, peList, vmScheduler);
		setPowerModel(powerModel);
	}

	public MergedHost(
			int id,
			RamProvisioner ramProvisioner,
			BwProvisioner bwProvisioner,
			long storage,
			List<? extends Pe> peList,
			VmScheduler vmScheduler) {
		super(id, ramProvisioner, bwProvisioner, storage, peList, vmScheduler);

		packetrecieved = new ArrayList<NetworkPacket>();
		packetTosendGlobal = new ArrayList<NetworkPacket>();
		packetTosendLocal = new ArrayList<NetworkPacket>();

	}


	@Override
	public double updateVmsProcessing(double currentTime) {
		double smallerTime = super.updateVmsProcessing(currentTime);
		setPreviousUtilizationMips(getUtilizationMips());
		setUtilizationMips(0);
		double hostTotalRequestedMips = 0;

		// double smallerTime = Double.MAX_VALUE;
		// insert in each vm packet recieved
		recvpackets();
		for (Vm vm : super.getVmList()) {
			double time = ((MergedVm) vm).updateVmProcessing(currentTime, getVmScheduler()
					.getAllocatedMipsForVm(vm));
			if (time > 0.0 && time < smallerTime) {
				smallerTime = time;
			}
		}
		// send the packets to other hosts/VMs
		sendpackets();

		for (Vm vm : getVmList()) {
			getVmScheduler().deallocatePesForVm(vm);
		}

		for (Vm vm : getVmList()) {
			getVmScheduler().allocatePesForVm(vm, vm.getCurrentRequestedMips());
		}

		for (Vm vm : getVmList()) {
			double totalRequestedMips = vm.getCurrentRequestedTotalMips();
			double totalAllocatedMips = getVmScheduler().getTotalAllocatedMipsForVm(vm);

			if (!Log.isDisabled()) {
				Log.formatLine(
						"%.2f: [Host #" + getId() + "] Total allocated MIPS for VM #" + vm.getId()
								+ " (Host #" + vm.getHost().getId()
								+ ") is %.2f, was requested %.2f out of total %.2f (%.2f%%)",
						CloudSim.clock(),
						totalAllocatedMips,
						totalRequestedMips,
						vm.getMips(),
						totalRequestedMips / vm.getMips() * 100);

				List<Pe> pes = getVmScheduler().getPesAllocatedForVM(vm);
				StringBuilder pesString = new StringBuilder();
				for (Pe pe : pes) {
					pesString.append(String.format(" PE #" + pe.getId() + ": %.2f.", pe.getPeProvisioner()
							.getTotalAllocatedMipsForVm(vm)));
				}
				Log.formatLine(
						"%.2f: [Host #" + getId() + "] MIPS for VM #" + vm.getId() + " by PEs ("
								+ getNumberOfPes() + " * " + getVmScheduler().getPeCapacity() + ")."
								+ pesString,
						CloudSim.clock());
			}

			if (getVmsMigratingIn().contains(vm)) {
				Log.formatLine("%.2f: [Host #" + getId() + "] VM #" + vm.getId()
						+ " is being migrated to Host #" + getId(), CloudSim.clock());
			} else {
				if (totalAllocatedMips + 0.1 < totalRequestedMips) {
					Log.formatLine("%.2f: [Host #" + getId() + "] Under allocated MIPS for VM #" + vm.getId()
							+ ": %.2f", CloudSim.clock(), totalRequestedMips - totalAllocatedMips);
				}

				vm.addStateHistoryEntry(
						currentTime,
						totalAllocatedMips,
						totalRequestedMips,
						(vm.isInMigration() && !getVmsMigratingIn().contains(vm)));

				if (vm.isInMigration()) {
					Log.formatLine(
							"%.2f: [Host #" + getId() + "] VM #" + vm.getId() + " is in migration",
							CloudSim.clock());
					totalAllocatedMips /= 0.9; // performance degradation due to migration - 10%
				}
			}

			setUtilizationMips(getUtilizationMips() + totalAllocatedMips);
			hostTotalRequestedMips += totalRequestedMips;
		}

		addStateHistoryEntry(
				currentTime,
				getUtilizationMips(),
				hostTotalRequestedMips,
				(getUtilizationMips() > 0));

		return smallerTime;

	}

	/**
	 * Receives packets and forward them to the corresponding VM.
	 */
	private void recvpackets() {
		for (NetworkPacket hs : packetrecieved) {
			hs.pkt.recievetime = CloudSim.clock();

			// insert the packet in recievedlist of VM
			Vm vm = VmList.getById(getVmList(), hs.pkt.reciever);
			List<HostPacket> pktlist = ((NetworkCloudletSpaceSharedScheduler) vm.getCloudletScheduler()).pktrecv
					.get(hs.pkt.sender);

			if (pktlist == null) {
				pktlist = new ArrayList<HostPacket>();
				((NetworkCloudletSpaceSharedScheduler) vm.getCloudletScheduler()).pktrecv.put(
						hs.pkt.sender,
						pktlist);

			}
			pktlist.add(hs.pkt);

		}
		packetrecieved.clear();
	}

	/**
	 * Sends packets checks whether a packet belongs to a local VM or to a
	 * VM hosted on other machine.
	 */
	private void sendpackets() {
		for (Vm vm : super.getVmList()) {
			for (Entry<Integer, List<HostPacket>> es : ((NetworkCloudletSpaceSharedScheduler) vm
					.getCloudletScheduler()).pkttosend.entrySet()) {
				List<HostPacket> pktlist = es.getValue();
				for (HostPacket pkt : pktlist) {
					NetworkPacket hpkt = new NetworkPacket(getId(), pkt, vm.getId(), pkt.sender);
					Vm vm2 = VmList.getById(this.getVmList(), hpkt.recievervmid);
					if (vm2 != null) {
						packetTosendLocal.add(hpkt);
					} else {
						packetTosendGlobal.add(hpkt);
					}
				}
				pktlist.clear();
			}
		}

		boolean flag = false;

		for (NetworkPacket hs : packetTosendLocal) {
			flag = true;
			hs.stime = hs.rtime;
			hs.pkt.recievetime = CloudSim.clock();
			// insertthe packet in recievedlist
			Vm vm = VmList.getById(getVmList(), hs.pkt.reciever);

			List<HostPacket> pktlist = ((NetworkCloudletSpaceSharedScheduler) vm.getCloudletScheduler()).pktrecv
					.get(hs.pkt.sender);
			if (pktlist == null) {
				pktlist = new ArrayList<HostPacket>();
				((NetworkCloudletSpaceSharedScheduler) vm.getCloudletScheduler()).pktrecv.put(
						hs.pkt.sender,
						pktlist);
			}
			pktlist.add(hs.pkt);
		}
		if (flag) {
			for (Vm vm : super.getVmList()) {
				vm.updateVmProcessing(CloudSim.clock(), getVmScheduler().getAllocatedMipsForVm(vm));
			}
		}

		// Sending packet to other VMs therefore packet is forwarded to a Edge switch
		packetTosendLocal.clear();
		double avband = bandwidth / packetTosendGlobal.size();
		for (NetworkPacket hs : packetTosendGlobal) {
			double delay = (1000 * hs.pkt.data) / avband;
			NetworkConstants.totaldatatransfer += hs.pkt.data;

			CloudSim.send(getDatacenter().getId(), sw.getId(), delay, CloudSimTags.Network_Event_UP, hs);
			// send to switch with delay
		}
		packetTosendGlobal.clear();
	}

	/**
	 * Gets the maximum utilization among the PEs of a given VM.
	 * @param vm The VM to get its PEs maximum utilization
	 * @return The maximum utilization among the PEs of the VM.
	 */
	public double getMaxUtilizationAmongVmsPes(Vm vm) {
		return PeList.getMaxUtilizationAmongVmsPes(getPeList(), vm);
	}

/**
 * HostDynamicWorkload methods
 */
	/**
	 * Gets the list of completed vms.
	 *
	 * @return the completed vms
	 */
	public List<Vm> getCompletedVms() {
		List<Vm> vmsToRemove = new ArrayList<Vm>();
		for (Vm vm : getVmList()) {
			if (vm.isInMigration()) {
				continue;
			}
			if (vm.getCurrentRequestedTotalMips() == 0) {
				vmsToRemove.add(vm);
			}
		}
		return vmsToRemove;
	}

	/**
	 * Gets the max utilization percentage among by all PEs.
	 *
	 * @return the maximum utilization percentage
	 */
	public double getMaxUtilization() {
		return PeList.getMaxUtilization(getPeList());
	}

	/**
	 * Gets the utilization of memory (in absolute values).
	 *
	 * @return the utilization of memory
	 */
	public double getUtilizationOfRam() {
		return getRamProvisioner().getUsedRam();
	}

	/**
	 * Gets the utilization of bw (in absolute values).
	 *
	 * @return the utilization of bw
	 */
	public double getUtilizationOfBw() {
		return getBwProvisioner().getUsedBw();
	}

	/**
	 * Get current utilization of CPU in percentage.
	 *
	 * @return current utilization of CPU in percents
	 */
	public double getUtilizationOfCpu() {
		double utilization = getUtilizationMips() / getTotalMips();
		if (utilization > 1 && utilization < 1.01) {
			utilization = 1;
		}
		return utilization;
	}

	/**
	 * Gets the previous utilization of CPU in percentage.
	 *
	 * @return the previous utilization of cpu in percents
	 */
	public double getPreviousUtilizationOfCpu() {
		double utilization = getPreviousUtilizationMips() / getTotalMips();
		if (utilization > 1 && utilization < 1.01) {
			utilization = 1;
		}
		return utilization;
	}

	/**
	 * Get current utilization of CPU in MIPS.
	 *
	 * @return current utilization of CPU in MIPS
	 * @todo This method only calls the  {@link #getUtilizationMips()}.
	 * getUtilizationMips may be deprecated and its code copied here.
	 */
	public double getUtilizationOfCpuMips() {
		return getUtilizationMips();
	}

	/**
	 * Gets the utilization of CPU in MIPS.
	 *
	 * @return current utilization of CPU in MIPS
	 */
	public double getUtilizationMips() {
		return utilizationMips;
	}

	/**
	 * Sets the utilization mips.
	 *
	 * @param utilizationMips the new utilization mips
	 */
	protected void setUtilizationMips(double utilizationMips) {
		this.utilizationMips = utilizationMips;
	}

	/**
	 * Gets the previous utilization of CPU in mips.
	 *
	 * @return the previous utilization of CPU in mips
	 */
	public double getPreviousUtilizationMips() {
		return previousUtilizationMips;
	}

	/**
	 * Sets the previous utilization of CPU in mips.
	 *
	 * @param previousUtilizationMips the new previous utilization of CPU in mips
	 */
	protected void setPreviousUtilizationMips(double previousUtilizationMips) {
		this.previousUtilizationMips = previousUtilizationMips;
	}

	/**
	 * Gets the host state history.
	 *
	 * @return the state history
	 */
	public List<HostStateHistoryEntry> getStateHistory() {
		return stateHistory;
	}

	/**
	 * Adds a host state history entry.
	 *
	 * @param time the time
	 * @param allocatedMips the allocated mips
	 * @param requestedMips the requested mips
	 * @param isActive the is active
	 */
	public void addStateHistoryEntry(double time, double allocatedMips, double requestedMips, boolean isActive) {

		HostStateHistoryEntry newState = new HostStateHistoryEntry(
				time,
				allocatedMips,
				requestedMips,
				isActive);
		if (!getStateHistory().isEmpty()) {
			HostStateHistoryEntry previousState = getStateHistory().get(getStateHistory().size() - 1);
			if (previousState.getTime() == time) {
				getStateHistory().set(getStateHistory().size() - 1, newState);
				return;
			}
		}
		getStateHistory().add(newState);
	}

/**
 * Power methods
 */
	/**
	 * Gets the power. For this moment only consumed by all PEs.
	 * 
	 * @return the power
	 */
	public double getPower() {
		return getPower(getUtilizationOfCpu());
	}

	/**
	 * Gets the current power consumption of the host. For this moment only consumed by all PEs.
	 * 
	 * @param utilization the utilization percentage (between [0 and 1]) of a resource that
         * is critical for power consumption
	 * @return the power consumption
	 */
	protected double getPower(double utilization) {
		double power = 0;
		try {
			power = getPowerModel().getPower(utilization);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		return power;
	}

	/**
	 * Gets the max power that can be consumed by the host.
	 * 
	 * @return the max power
	 */
	public double getMaxPower() {
		double power = 0;
		try {
			power = getPowerModel().getPower(1);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		return power;
	}

	/**
	 * Gets the energy consumption using linear interpolation of the utilization change.
	 * 
	 * @param fromUtilization the initial utilization percentage
	 * @param toUtilization the final utilization percentage
	 * @param time the time
	 * @return the energy
	 */
	public double getEnergyLinearInterpolation(double fromUtilization, double toUtilization, double time) {
		if (fromUtilization == 0) {
			return 0;
		}
		double fromPower = getPower(fromUtilization);
		double toPower = getPower(toUtilization);
		return (fromPower + (toPower - fromPower) / 2) * time;
	}

	/**
	 * Sets the power model.
	 * 
	 * @param powerModel the new power model
	 */
	protected void setPowerModel(PowerModel powerModel) {
		this.powerModel = powerModel;
	}

	/**
	 * Gets the power model.
	 * 
	 * @return the power model
	 */
	public PowerModel getPowerModel() {
		return powerModel;
	}

}
