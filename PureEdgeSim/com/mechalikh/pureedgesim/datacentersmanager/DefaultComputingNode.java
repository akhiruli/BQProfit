/**
 *     PureEdgeSim:  A Simulation Framework for Performance Evaluation of Cloud, Edge and Mist Computing Environments 
 *
 *     This file is part of PureEdgeSim Project.
 *
 *     PureEdgeSim is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     PureEdgeSim is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with PureEdgeSim. If not, see <http://www.gnu.org/licenses/>.
 *     
 *     @author Charafeddine Mechalikh
 **/
package com.mechalikh.pureedgesim.datacentersmanager;

import java.util.LinkedList;

import BQProfit.Helper;
import org.jgrapht.GraphPath;

import com.mechalikh.pureedgesim.network.NetworkLink;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.simulationengine.Event;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import BQProfit.block_chain.Blockchain;

public class DefaultComputingNode extends LocationAwareNode {
	protected int applicationType;
	protected boolean isSensor = false;
	protected double availableStorage = 0;
	protected double Storage = 0;
	protected boolean isIdle = true;
	protected double tasks = 0;
	protected double totalTasks = 0;
	protected double totalMipsCapacity;
	protected double mipsPerCore;
	protected double numberOfCPUCores;
	protected double availableCores;
	protected String storageType;
	protected long readBandwidth;
	protected long writeBandwidth;
	protected double dataBusBandwidth;
	protected boolean ssdEnabled;
	protected double ssdStorage;
	protected double ssdReadBw;
	protected double ssdWriteBw;
	protected String serverType;
	protected boolean isLeader = false;
	protected Blockchain blockchain = null;

	protected CostPlan costPlan = null;

	protected long ram;

	public String getStorageType() {
		return storageType;
	}
	public void setStorageType(String storageType) {
		this.storageType = storageType;
	}

	public long getReadBandwidth() {
		return readBandwidth;
	}
	public void setReadBandwidth(long readBandwidth) {
		this.readBandwidth = readBandwidth;
	}
	public long getWriteBandwidth() {
		return writeBandwidth;
	}
	public void setWriteBandwidth(long writeBandwidth) {
		this.writeBandwidth = writeBandwidth;
	}
	public boolean isSsdEnabled() {
		return ssdEnabled;
	}

	public void setSsdEnabled(boolean ssdEnabled) {
		this.ssdEnabled = ssdEnabled;
	}

	public double getSsdStorage() {
		return ssdStorage;
	}

	public void setSsdStorage(double ssdStorage) {
		this.ssdStorage = ssdStorage;
	}

	public double getSsdReadBw() {
		return ssdReadBw;
	}

	public void setSsdReadBw(double ssdReadBw) {
		this.ssdReadBw = ssdReadBw;
	}

	public double getSsdWriteBw() {
		return ssdWriteBw;
	}

	public void setSsdWriteBw(double ssdWriteBw) {
		this.ssdWriteBw = ssdWriteBw;
	}
	protected LinkedList<Task> tasksQueue = new LinkedList<>();
	protected static final int EXECUTION_FINISHED = 2;

	public DefaultComputingNode(SimulationManager simulationManager, double mipsPerCore, long numberOfCPUCores,
			long storage) {
		super(simulationManager);
		setStorage(storage);
		setAvailableStorage(storage);
		setTotalMipsCapacity(mipsPerCore * numberOfCPUCores);
		this.mipsPerCore = mipsPerCore;
		setNumberOfCPUCores(numberOfCPUCores);
		availableCores = numberOfCPUCores;
		ssdEnabled = false;
		if (mipsPerCore <= 0 || numberOfCPUCores <= 0 || storage <= 0)
			this.setAsSensor(true);
	}

	@Override
	public void processEvent(Event e) {
		switch (e.getTag()) {
		case EXECUTION_FINISHED:
			executionFinished(e);
			break;
		default:
			super.processEvent(e);
			break;
		}
	}

	public double getNumberOfCPUCores() {
		return numberOfCPUCores;
	}

	public void setNumberOfCPUCores(double numberOfCPUCores) {
		this.numberOfCPUCores = numberOfCPUCores;
	}

	public int getApplicationType() {
		return applicationType;
	}

	public void setApplicationType(int applicationType) {
		this.applicationType = applicationType;
	}

	public double getAvailableStorage() {
		return availableStorage;
	}

	public void setAvailableStorage(double availableStorage) {
		this.availableStorage = availableStorage;
	}

	public double getAvgCpuUtilization() {
		if (this.getTotalMipsCapacity() == 0)
			return 0;
		double utilization = totalTasks * 100 / (getTotalMipsCapacity() * simulationManager.getSimulation().clock());
		return Math.min(100, utilization);
	}

	public double getCurrentCpuUtilization() {
		if (this.getTotalMipsCapacity() == 0)
			return 0;
		double utilization = tasks * 100 / getTotalMipsCapacity();
		return utilization > 100 ? 100 : utilization;
	}

	public boolean isIdle() {
		return isIdle;
	}

	public void setIdle(boolean isIdle) {
		this.isIdle = isIdle;
	}

	public void addCpuUtilization(Task task) {
		tasks += task.getLength();
		totalTasks += task.getLength();
		setIdle(false);
	}

	public void removeCpuUtilization(Task task) {
		tasks -= task.getLength();
		if (tasks <= 0)
			setIdle(true);
	}

	public boolean isSensor() {
		return isSensor;
	}

	public void setAsSensor(boolean isSensor) {
		this.isSensor = isSensor;
	}

	public LinkedList<Task> getTasksQueue() {
		return tasksQueue;
	}

	public double getTotalStorage() {
		return Storage;
	}

	public void setStorage(double storage) {
		Storage = storage;
	}

	public double getTotalMipsCapacity() {
		return totalMipsCapacity;
	}

	public void setTotalMipsCapacity(double totalMipsCapacity) {
		this.totalMipsCapacity = totalMipsCapacity;
	}
	public double getDataBusBandwidth() {
		return dataBusBandwidth;
	}
	public void setDataBusBandwidth(double dataBusBandwidth) {
		this.dataBusBandwidth = dataBusBandwidth;
	}
	public void setServerType(String serverType) {
		this.serverType = serverType;
		if(this.getServerType() != null && this.getServerType().equals("consensus_node")){
			blockchain = new Blockchain();
		}
	}
	public String getServerType(){return this.serverType;}
	public boolean isLeader() {
		return isLeader;
	}

	public void setLeader(boolean leader) {
		isLeader = leader;
	}
	public Blockchain getBlockchain() {
		return blockchain;
	}

	public void setBlockchain(Blockchain blockchain) {
		this.blockchain = blockchain;
	}

	public CostPlan getCostPlan() {
		return costPlan;
	}

	public void setCostPlan(CostPlan costPlan) {
		this.costPlan = costPlan;
	}
	public long getRam() {
		return ram;
	}

	public void setRam(long ram) {
		this.ram = ram;
	}

	@Override
	public void submitTask(Task task) {
		// The task to be executed has been received, save the arrival time
		task.setArrivalTime(getSimulation().clock());
		if (this.availableCores > 0) {
			startExecution(task);
		}
		// Otherwise, add it to the execution queue
		else {
			getTasksQueue().add(task);
		}

	}

	private void startExecution(Task task) {
		// Update the CPU utilization
		addCpuUtilization(task);
		availableCores--;
		task.setExecutionStartTime(getSimulation().clock());

		/*
		 * Arguably, the correct way to get energy consumption measurement is to place
		 * the following line of code within the processEvent(Event e) method of the
		 * EnergyAwareComputingNode:
		 * 
		 * getEnergyModel().updateCpuEnergyConsumption(getCurrentCpuUtilization());
		 * 
		 * I mean, this makes sense right?. The problem with this is that it will depend
		 * on the update interval. To get more accurate results, you need to set the
		 * update interval as low as possible, this will in turn increase the simulation
		 * duration, which is clearly not convenient. One way around it, is to make the
		 * measurement here, when the task is being executed. The problem with this is
		 * that if we don't receive a task, the static energy consumption will not be
		 * measured. So the best approach is to measure the dynamic one here, and add
		 * the static one there.
		 */

		double executionTime = 0;
		if(this.getType() == SimulationParameters.TYPES.EDGE_DEVICE){
			if(this.simulationManager.getScenario().getStringOrchAlgorithm().equals("BQPROFIT_ALGO")) {
				double maxLatency = 1;
				if(task.getMaxLatency() > 0){
					maxLatency = task.getMaxLatency();
				}
				double minMipsNeeded = Math.ceil(task.getLength() / maxLatency);
				if (minMipsNeeded > mipsPerCore) {
					executionTime = task.getLength() / mipsPerCore;
				} else {
					executionTime = task.getLength() / minMipsNeeded;
				}
				//getEnergyModel().updateDynamicEnergyConsumption(task.getLength(), mipsPerCore);
				getEnergyModel().updateDynamicEnergyConsumption(mipsPerCore, minMipsNeeded, executionTime);
			} else{
				executionTime = task.getLength() / mipsPerCore;
				//getEnergyModel().updateDynamicEnergyConsumption(task.getLength(), mipsPerCore);
				getEnergyModel().updateDynamicEnergyConsumption(mipsPerCore, mipsPerCore, executionTime);
			}
		} else {
			executionTime = Helper.calculateExecutionTime(this, task);
			//getEnergyModel().updateDynamicEnergyConsumption(task.getLength(), mipsPerCore);
			getEnergyModel().updateDynamicEnergyConsumption(mipsPerCore, mipsPerCore, executionTime);
		}
		schedule(this, executionTime, EXECUTION_FINISHED, task);
//		schedule(this, ((double) task.getLength() / mipsPerCore), EXECUTION_FINISHED, task);
	}

	public double getMipsPerCore() {
		return mipsPerCore;
	}

	private void executionFinished(Event e) {
		// The execution of one task has been finished, free one more CPU core.

		availableCores++;

		// Update CPU utilization.
		removeCpuUtilization((Task) e.getData());

		// Save the execution end time for later use.
		((Task) e.getData()).setExecutionFinishTime(this.getSimulation().clock());

		// Notify the simulation manager that a task has been finished, and it's time to
		// return the execution results.
		scheduleNow(simulationManager, SimulationManager.TRANSFER_RESULTS_TO_ORCH, e.getData());

		// If there are tasks waiting for execution
		if (!getTasksQueue().isEmpty()) {

			// Execute the first task in the queue on the available core.
			Task task = getTasksQueue().getFirst();

			// Remove the task from the queue.
			getTasksQueue().removeFirst();

			// Execute the task.
			startExecution(task);
		}
	}

	@Override
	public void setApplicationPlacementLocation(ComputingNode node) {
		this.applicationPlacementLocation = node;
		//this.isApplicationPlaced = true;
		if (this != node) {
			if (node.getType() == SimulationParameters.TYPES.EDGE_DEVICE) {
				simulationManager.getDataCentersManager().getTopology().removeLink(currentDeviceToDeviceWifiLink);
				currentDeviceToDeviceWifiLink.setDst(node);
				simulationManager.getDataCentersManager().getTopology().addLink(currentDeviceToDeviceWifiLink);
			}

			GraphPath<ComputingNode, NetworkLink> path = simulationManager.getDataCentersManager().getTopology()
					.getPath(this, node);

			vertexList.addAll(path.getVertexList());
			edgeList.addAll(path.getEdgeList());
		}
	}
}
