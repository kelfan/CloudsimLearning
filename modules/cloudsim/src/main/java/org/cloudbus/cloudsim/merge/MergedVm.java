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
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.network.datacenter.NetworkCloudlet;
import org.cloudbus.cloudsim.util.MathUtil;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
public class MergedVm extends Vm implements Comparable<Object> {

	/** The Constant HISTORY_LENGTH. */
	public static final int HISTORY_LENGTH = 30;

	/** The CPU utilization percentage history. */
	private final List<Double> utilizationHistory = new LinkedList<Double>();

	/** The previous time that cloudlets were processed. */
	private double previousTime;

	/** The scheduling interval to update the processing of cloudlets
	 * running in this VM. */
	private double schedulingInterval;

        /**
         * List of {@link NetworkCloudlet} of the VM.
         */
	public ArrayList<NetworkCloudlet> cloudletlist;

        /**
         * @todo It doesn't appear to be used.
         */
	int type;

        /**
         * List of packets received by the VM.
         */
	public ArrayList<HostPacket> recvPktlist;

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

	public MergedVm(
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

		cloudletlist = new ArrayList<NetworkCloudlet>();
	}

	/**
	 * Instantiates a new PowerVm.
	 *
	 * @param id the id
	 * @param userId the user id
	 * @param mips the mips
	 * @param pesNumber the pes number
	 * @param ram the ram
	 * @param bw the bw
	 * @param size the size
	 * @param priority the priority
	 * @param vmm the vmm
	 * @param cloudletScheduler the cloudlet scheduler
	 * @param schedulingInterval the scheduling interval
	 */
	public MergedVm(
			final int id,
			final int userId,
			final double mips,
			final int pesNumber,
			final int ram,
			final long bw,
			final long size,
			final int priority,
			final String vmm,
			final CloudletScheduler cloudletScheduler,
			final double schedulingInterval) {
		super(id, userId, mips, pesNumber, ram, bw, size, vmm, cloudletScheduler);
		setSchedulingInterval(schedulingInterval);
	}

	public boolean isFree() {
		return flagfree;
	}

	@Override
	public int compareTo(Object arg0) {
		MergedVm hs = (MergedVm) arg0;
		if (hs.finishtime > finishtime) {
			return -1;
		}
		if (hs.finishtime < finishtime) {
			return 1;
		}
		return 0;
	}

	@Override
	public double updateVmProcessing(final double currentTime, final List<Double> mipsShare) {
		double time = super.updateVmProcessing(currentTime, mipsShare);
		if (currentTime > getPreviousTime() && (currentTime - 0.1) % getSchedulingInterval() == 0) {
			double utilization = getTotalUtilizationOfCpu(getCloudletScheduler().getPreviousTime());
			if (CloudSim.clock() != 0 || utilization != 0) {
				addUtilizationHistoryValue(utilization);
			}
			setPreviousTime(currentTime);
		}
		return time;
	}

	/**
	 * Gets the utilization MAD in MIPS.
	 *
	 * @return the utilization MAD in MIPS
	 */
	public double getUtilizationMad() {
		double mad = 0;
		if (!getUtilizationHistory().isEmpty()) {
			int n = HISTORY_LENGTH;
			if (HISTORY_LENGTH > getUtilizationHistory().size()) {
				n = getUtilizationHistory().size();
			}
			double median = MathUtil.median(getUtilizationHistory());
			double[] deviationSum = new double[n];
			for (int i = 0; i < n; i++) {
				deviationSum[i] = Math.abs(median - getUtilizationHistory().get(i));
			}
			mad = MathUtil.median(deviationSum);
		}
		return mad;
	}

	/**
	 * Gets the utilization mean in percents.
	 *
	 * @return the utilization mean in MIPS
	 */
	public double getUtilizationMean() {
		double mean = 0;
		if (!getUtilizationHistory().isEmpty()) {
			int n = HISTORY_LENGTH;
			if (HISTORY_LENGTH > getUtilizationHistory().size()) {
				n = getUtilizationHistory().size();
			}
			for (int i = 0; i < n; i++) {
				mean += getUtilizationHistory().get(i);
			}
			mean /= n;
		}
		return mean * getMips();
	}

	/**
	 * Gets the utilization variance in MIPS.
	 *
	 * @return the utilization variance in MIPS
	 */
	public double getUtilizationVariance() {
		double mean = getUtilizationMean();
		double variance = 0;
		if (!getUtilizationHistory().isEmpty()) {
			int n = HISTORY_LENGTH;
			if (HISTORY_LENGTH > getUtilizationHistory().size()) {
				n = getUtilizationHistory().size();
			}
			for (int i = 0; i < n; i++) {
				double tmp = getUtilizationHistory().get(i) * getMips() - mean;
				variance += tmp * tmp;
			}
			variance /= n;
		}
		return variance;
	}

	/**
	 * Adds a CPU utilization percentage history value.
	 *
	 * @param utilization the CPU utilization percentage to add
	 */
	public void addUtilizationHistoryValue(final double utilization) {
		getUtilizationHistory().add(0, utilization);
		if (getUtilizationHistory().size() > HISTORY_LENGTH) {
			getUtilizationHistory().remove(HISTORY_LENGTH);
		}
	}

	/**
	 * Gets the CPU utilization percentage history.
	 *
	 * @return the CPU utilization percentage history
	 */
	public List<Double> getUtilizationHistory() {
		return utilizationHistory;
	}

	/**
	 * Gets the previous time.
	 *
	 * @return the previous time
	 */
	public double getPreviousTime() {
		return previousTime;
	}

	/**
	 * Sets the previous time.
	 *
	 * @param previousTime the new previous time
	 */
	public void setPreviousTime(final double previousTime) {
		this.previousTime = previousTime;
	}

	/**
	 * Gets the scheduling interval.
	 *
	 * @return the schedulingInterval
	 */
	public double getSchedulingInterval() {
		return schedulingInterval;
	}

	/**
	 * Sets the scheduling interval.
	 *
	 * @param schedulingInterval the schedulingInterval to set
	 */
	protected void setSchedulingInterval(final double schedulingInterval) {
		this.schedulingInterval = schedulingInterval;
	}

}
