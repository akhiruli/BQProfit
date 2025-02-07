package BQProfit;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.datacentersmanager.DataCenter;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import com.mechalikh.pureedgesim.taskorchestrator.Orchestrator;
import data_processor.DataProcessor;
import data_processor.Job;
import BQProfit.dag.TaskNode;

import java.util.*;

import static com.mechalikh.pureedgesim.scenariomanager.SimulationParameters.TYPES.*;

public class CustomOrchestrator extends Orchestrator {
    protected Map<Integer, Integer> historyMap = new HashMap<>();
    private SimulationManager simManager;
    private Integer currDcIndex;

    public CustomOrchestrator(SimulationManager simulationManager) {
        super(simulationManager);
        simManager = simulationManager;
        // Initialize the history map
        for (int i = 0; i < nodeList.size(); i++)
            historyMap.put(i, 0);
        currDcIndex = 0;
    }

    protected ComputingNode findComputingNode(String[] architecture, Task task) {
            if ("TRADE_OFF".equals(algorithm)) {
                return tradeOff(architecture, task);
            }else if ("BQPROFIT_ALGO".equals(algorithm)) {
                return lbdPolicy(task);
            } else if ("AUCTION_BASED".equals(algorithm)) {
                return auctionBasedPolicy(task);
            } else if ("MKL_RR".equals(algorithm)) {
                return mklRRPolicy(task);
            } else if ("IMOBWO".equals(algorithm)) {
                return imobwopolicy(task);
            } else {
                throw new IllegalArgumentException(getClass().getSimpleName() + " - Unknown orchestration algorithm '"
                        + algorithm + "', please check the simulation parameters file...");
            }

    }

    private ComputingNode imobwopolicy(Task task) {
        TaskNode taskNode = (TaskNode)task;
        if(taskNode.getTaskDecision() == TaskNode.TaskDecision.UE_ONLY){
            return taskNode.getEdgeDevice();
        } else if (taskNode.getTaskDecision() == TaskNode.TaskDecision.MEC_ONLY) {
            return taskNode.getSelectedComputingNode();
        } else {
            return simManager.getDataCentersManager().getCloudDatacentersList().get(0).nodeList.get(0);
        }
    }

    private ComputingNode mklRRPolicy(Task task) {
        TaskNode taskNode = (TaskNode)task;
        if(taskNode.getTaskDecision() == TaskNode.TaskDecision.UE_ONLY){
            return taskNode.getEdgeDevice();
        }else{
            List<DataCenter> edgeDcs = simManager.getDataCentersManager().getEdgeDatacenterList();
            List<DataCenter> cloudDcs = simManager.getDataCentersManager().getCloudDatacentersList();

            Integer total_dc_size = edgeDcs.size() + cloudDcs.size();
            ComputingNode computingNode = null;

            Integer server_index = 0;
            if(currDcIndex == total_dc_size -1){ //cloud DC
                server_index = Math.toIntExact(Helper.getRandomInteger(0, cloudDcs.get(0).nodeList.size()-1));
                computingNode = cloudDcs.get(0).nodeList.get(server_index);
            } else{ //edge DC
                server_index = Math.toIntExact(Helper.getRandomInteger(0, edgeDcs.get(currDcIndex).nodeList.size()-1));
                computingNode = edgeDcs.get(currDcIndex).nodeList.get(server_index);
            }

            currDcIndex++;
            if(currDcIndex >= total_dc_size){
                currDcIndex = 0;
            }

            return computingNode;
        }

    }

    private ComputingNode auctionBasedPolicy(Task task) {
        TaskNode taskNode = (TaskNode)task;
        if(taskNode.getTaskDecision() == TaskNode.TaskDecision.UE_ONLY){
            return taskNode.getEdgeDevice();
        } else {
            if (taskNode.getSelectedComputingNode() != null) {
                return taskNode.getSelectedComputingNode();
            } else {
                return taskNode.getEdgeDevice();
            }
        }
    }

    private ComputingNode lbdPolicy(Task task) {
        TaskNode taskNode = (TaskNode)task;
        //System.out.println("Allocating computing resources: "+taskNode.getApplicationID()+" : "+taskNode.getId());
        if(taskNode.getTaskDecision() == TaskNode.TaskDecision.UE_ONLY){
            return taskNode.getEdgeDevice();
        } else{
            return taskNode.getSelectedComputingNode();
        }
    }

    protected ComputingNode tradeOff(String[] architecture, Task task) {
        int selected = -1;
        double min = -1;
        double new_min;// the computing node with minimum weight;
        ComputingNode node;
        // get best computing node for this task
        for (int i = 0; i < nodeList.size(); i++) {
            node = nodeList.get(i);
            if(task.getId() == 0 && task.getEdgeDevice() == node){
                selected = i;
                break;
            }
            if (offloadingIsPossible(task, node, architecture)) {
                // the weight below represent the priority, the less it is, the more it is
                // suitable for offlaoding, you can change it as you want
                double weight = 1.2; // this is an edge server 'cloudlet', the latency is slightly high then edge
                // devices
                if (node.getType() == SimulationParameters.TYPES.CLOUD) {
                    weight = 1.8; // this is the cloud, it consumes more energy and results in high latency, so
                    // better to avoid it
                } else if (node.getType() == EDGE_DEVICE) {
                    weight = 1.3;// this is an edge device, it results in an extremely low latency, but may
                    // consume more energy.
                }
                new_min = (historyMap.get(i) + 1) * weight * task.getLength() / node.getMipsPerCore();
                if (min == -1 || min > new_min) { // if it is the first iteration, or if this computing node has more
                    // cpu mips and
                    // less waiting tasks
                    min = new_min;
                    // set the first computing node as the best one
                    selected = i;
                }
            }
        }
        if (selected != -1) {
            historyMap.put(selected, historyMap.get(selected) + 1);
            node = nodeList.get(selected);
        } else{
            node = null;
        }
        // assign the tasks to the selected computing node

        return node;
    }

    @Override
    public void resultsReturned(Task task) {
        TaskNode taskNode = (TaskNode) task;
        taskNode.setTaskDone(true);
        CustomSimMananger customSimMananger = (CustomSimMananger) simManager;
        if(task.getStatus() == Task.Status.FAILED) {
            System.out.println("Task " + task.getId() + " failed for job " + task.getApplicationID() + " CPU: " + task.getLength() + " node type: " + task.getComputingNode().getType() + " ID:" + task.getComputingNode().getId() + " Reason : " + task.getFailureReason());
            if(task.getApplicationID() == 4 && task.getId() ==69){
                System.out.println("Successor: "+ taskNode.successors.size()+ " End: "+ taskNode.isEndTask());
            }
            if(task.getComputingNode().getType() == EDGE_DATACENTER || task.getComputingNode().getType() == CLOUD) {
                simManager.getProfitCalculator().updatePenalty(taskNode.getChargingPlan().getPenalty());
            }
        } else{
            if (task.getComputingNode().getType() == EDGE_DATACENTER || task.getComputingNode().getType() == CLOUD) {
                if (customSimMananger.getScenario().getStringOrchAlgorithm().equals("BQPROFIT_ALGO") ||
                        customSimMananger.getScenario().getStringOrchAlgorithm().equals("IMOBWO")) {
                        simManager.getProfitCalculator().updateCpuProfit(taskNode.getLength(),
                                task.getComputingNode().getCostPlan(), taskNode);
                        simManager.getProfitCalculator().updateMemoryProfit(taskNode.getMemoryNeed(),
                                task.getComputingNode().getCostPlan(), taskNode);
                        simManager.getProfitCalculator().updateIoProfit((double) (taskNode.getReadOps() + taskNode.getWriteOps()),
                                task.getComputingNode().getCostPlan(), taskNode);

                } else if (customSimMananger.getScenario().getStringOrchAlgorithm().equals("AUCTION_BASED")) {
                        simManager.getProfitCalculator().updateCpuProfitAuction(taskNode.getLength(),
                                task.getComputingNode().getCostPlan(), taskNode.getBidingValue());
                        simManager.getProfitCalculator().updateMemoryProfitAuction(taskNode.getMemoryNeed(),
                                task.getComputingNode().getCostPlan(), taskNode.getBidingValue());
                        simManager.getProfitCalculator().updateIoProfitAuction((double) (taskNode.getReadOps() + taskNode.getWriteOps()),
                                task.getComputingNode().getCostPlan(), taskNode.getBidingValue());

                } else if (customSimMananger.getScenario().getStringOrchAlgorithm().equals("MKL_RR")) {
                    simManager.getProfitCalculator().updateCpuProfitMKLRR(taskNode.getLength(),
                            task.getComputingNode().getCostPlan(), taskNode.getChargingPlan());
                    simManager.getProfitCalculator().updateMemoryProfitMKLRR(taskNode.getMemoryNeed(),
                            task.getComputingNode().getCostPlan(), taskNode.getChargingPlan());
                    simManager.getProfitCalculator().updateIoProfitMKLRR((double) (taskNode.getReadOps() + taskNode.getWriteOps()),
                            task.getComputingNode().getCostPlan(), taskNode.getChargingPlan());
                }
            }
        }

        if(customSimMananger.getScenario().getStringOrchAlgorithm().equals("BQPROFIT_ALGO")) {
            List<DataCenter> dataCenterList = simManager.getDataCentersManager().getEdgeDatacenterList();
            for (DataCenter dc : dataCenterList) {
                for (ComputingNode node : dc.nodeList) {
                    if (node.isLeader()) {
                        node.getBlockchain().addTask(taskNode);
                    }
                }
            }
        }

        if(taskNode.isEndTask()) {
            Job app = DataProcessor.scheduledJob.get(taskNode.getApplicationID());
            app.setStatus(true);

            if (customSimMananger.isAllDagCompleted()){
                customSimMananger.genReport();
            }
        } else if(!taskNode.isEndTask()) {
            customSimMananger.scheduleSuccessors(taskNode.successors);
        }

//        if(task.getApplicationID() == 4 && task.getId() ==69){
//            for (TaskNode taskNode1 : taskNode.successors) {
//               boolean val =  customSimMananger.areAllPredecesorTasksDone(taskNode1);
//               System.out.println(val);
//            }
//        }
    }

}
