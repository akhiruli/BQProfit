package BQProfit;

import com.mechalikh.pureedgesim.datacentersmanager.DataCentersManager;
import com.mechalikh.pureedgesim.scenariomanager.Scenario;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.simulationengine.PureEdgeSim;
import com.mechalikh.pureedgesim.simulationmanager.SimLog;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.simulationmanager.SimulationThread;
import data_processor.DataProcessor;
import data_processor.Job;
import BQProfit.auction_based.Auction;
import BQProfit.auction_based.Bid;
import BQProfit.auction_based.BidingValue;
import BQProfit.bqprofit.algorithm.Gene;
import BQProfit.bqprofit.algorithm.RaGeneticAlgorithm;
import BQProfit.dag.TaskNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CustomSimMananger extends SimulationManager {
    PureEdgeSim pureEdgeSim;
    Integer totalTaskSent;
    /**
     * Initializes the simulation manager.
     *
     * @param simLog       The simulation logger
     * @param pureEdgeSim  The CloudSim simulation engine.
     * @param simulationId The simulation ID
     * @param iteration    Which simulation run
     * @param scenario     The scenario is composed of the algorithm and
     *                     architecture that are being used, and the number of edge
     *                     devices.
     * @see SimulationThread#startSimulation()
     */
    public CustomSimMananger(SimLog simLog, PureEdgeSim pureEdgeSim, int simulationId, int iteration, Scenario scenario) {
        super(simLog, pureEdgeSim, simulationId, iteration, scenario);
        totalTaskSent = 0;
    }

    @Override
    public void startInternal() {
        // Initialize logger variables.
        simLog.setGeneratedTasks(tasksList.size());
        simLog.setCurrentOrchPolicy(scenario.getStringOrchArchitecture());

        simLog.print(getClass().getSimpleName() + " - Simulation: " + getSimulationId() + "  , iteration: "
                + getIteration());
        List<Integer> rootTaskIndexes = getRootTaskIndexes();
        for(Integer index : rootTaskIndexes) {
            schedule(this, tasksList.get(index).getTime(), SEND_TO_ORCH, tasksList.get(index));
        }

        totalTaskSent += rootTaskIndexes.size();
    }

    public void scheduleSuccessors(List<TaskNode> tasksList){
        List<TaskNode> taskLists = new ArrayList<>();
        for (TaskNode task : tasksList) {
            if (areAllPredecesorTasksDone(task)) {
                if(getScenario().getStringOrchAlgorithm().equals("BQPROFIT_ALGO")){
                    //taskLists.add(task);
                    schedule(this, task.getTime(), SEND_TO_ORCH, task);
                    totalTaskSent++;
                } else if (getScenario().getStringOrchAlgorithm().equals("AUCTION_BASED")) {
                    taskLists.add(task);
                } else {
                    //System.out.println("Scheduling- Task: "+task.getId() + " app_id: " + task.getApplicationID() + " : "  + task.getEdgeDevice().isApplicationPlaced());
                    schedule(this, task.getTime(), SEND_TO_ORCH, task);
                    totalTaskSent++;
                }
            }
        }

        if(taskLists.size() > 0){
            if(getScenario().getStringOrchAlgorithm().equals("BQPROFIT_ALGO")) {
                handleLbdAlgo(taskLists);
            } else if (getScenario().getStringOrchAlgorithm().equals("AUCTION_BASED")) {
                handleAuctionBasedAlgo(taskLists);
            }
        }
    }

    private void handleLbdAlgo(List<TaskNode> taskLists){
        RaGeneticAlgorithm geneticAlgorithm = new RaGeneticAlgorithm(this, taskLists);
        List<Gene> fittestIndividual = geneticAlgorithm.mapTaskToServer();
        Integer taskIndex = 0;
        for(Gene gene : fittestIndividual){
            DataCentersManager manager = this.getDataCentersManager();
            if(gene.getDcIndex() >= manager.getEdgeDatacenterList().size()){
                taskLists.get(taskIndex).setSelectedComputingNode(manager.getCloudDatacentersList().get(0).nodeList.get(0));
            } else{
                taskLists.get(taskIndex).setSelectedComputingNode(manager.getEdgeDatacenterList().get(gene.getDcIndex()).nodeList.get(gene.getServerIndex()));
            }

            schedule(this, tasksList.get(taskIndex).getTime(), SEND_TO_ORCH, tasksList.get(taskIndex));
            totalTaskSent++;
            ++taskIndex;
            if(taskIndex >= taskLists.size()){
                break;
            }
        }
    }

    private void handleAuctionBasedAlgo(List<TaskNode> taskLists) {
        Auction auction = new Auction(this);
        for(int i=0; i<taskLists.size();i++) {
            List<Bid> bids = new ArrayList<>();
            for (TaskNode taskNode : taskLists) {
                if (taskNode.getBidingValue() == null) {
                    BidingValue bidingValue = new BidingValue(Helper.getRandomDouble(0.0045, 0.0065),
                            Helper.getRandomDouble(0.00009, 0.0002), Helper.getRandomDouble(0.0009, 0.0015));
                    Bid bid = new Bid(taskNode, bidingValue);
                    taskNode.setBidingValue(bidingValue);
                    bids.add(bid);
                }

                taskNode.setMaxLatency(taskNode.getMaxLatency()-5);
            }

            auction.startAuction(bids);
        }

        for (TaskNode taskNode : taskLists) {
            schedule(this, taskNode.getTime(), SEND_TO_ORCH, taskNode);
            totalTaskSent++;
        }
    }

    public void genReport(){
        // Scheduling the end of the simulation.
        schedule(this, SimulationParameters.SIMULATION_TIME, PRINT_LOG);

        // Schedule the update of real-time charts.
        if (SimulationParameters.DISPLAY_REAL_TIME_CHARTS && !SimulationParameters.PARALLEL)
            scheduleNow(this, UPDATE_REAL_TIME_CHARTS);

        // Show simulation progress.
        scheduleNow(this, SHOW_PROGRESS);

        pureEdgeSim.setSimDone(true);
        simLog.printSameLine("Simulation progress : [", "red");
    }

    boolean areAllPredecesorTasksDone(TaskNode task){
        boolean ret = true;
        List<TaskNode> preds = task.predecessors;
        for(TaskNode taskNode : preds){
            if(!taskNode.isTaskDone()){
                ret = false;
                break;
            }
        }
        return ret;
    }

    public List<Integer> getRootTaskIndexes(){
        List<Integer> list_indexes = new ArrayList<>();
        for(int i = 0; i < tasksList.size(); i++){
            TaskNode taskNode = (TaskNode) tasksList.get(i);
            if(taskNode.isStartTask()){ //Assuming the task id of root task is 0
                list_indexes.add(i);
            }
        }
        return list_indexes;
    }
    public void setPureEdgeSim(PureEdgeSim pureEdgeSim) {
        this.pureEdgeSim = pureEdgeSim;
    }

    public boolean isAllDagCompleted() {
        boolean ret = true;
        for(Map.Entry<Integer, Job> entry : DataProcessor.scheduledJob.entrySet()){
            if(!entry.getValue().isStatus()){
                ret = false;
                break;
            }
        }
        return ret;
    }
}
