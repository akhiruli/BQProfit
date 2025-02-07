package BQProfit.IMOBWO.algorithm;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import BQProfit.Helper;
import BQProfit.dag.TaskNode;

public class MathFunctions {

    // Formula (3) from the paper
    public static double calculateCompletionTime(Task task, ComputingNode edgeServer) {
        double dnet = calculateNetworkDelay();
        double dira = calculateTransDelay(task, edgeServer);
        double dwait = calculateWaitingDelay(task, edgeServer);
        double dproc = Helper.calculateExecutionTime_new(edgeServer, task);
        return dnet+dira+dwait+dproc;
    }

    // Formula (4,5) from the paper
    public static double calculateNetworkDelay() {
        double networkIdleLatency=3;
        double utilizationRate = 0.5; // Random utilization rate
        double timeSlot = 1;
        return networkIdleLatency / (1-utilizationRate) *utilizationRate/ timeSlot;
    }

    // Formula (9, 10) from the paper
    public static double calculateTransDelay(Task task, ComputingNode computingNode){
        double distance = task.getEdgeDevice().getMobilityModel().distanceTo(computingNode);
        double upload_latency = Helper.getWirelessTransmissionLatency(task.getFileSize()) + Helper.calculatePropagationDelay(distance);
        double download_latency = Helper.getWirelessTransmissionLatency(task.getOutputSize()) + Helper.calculatePropagationDelay(distance);
        return upload_latency + download_latency;
    }

    private static Double getRevenue(Task task){
        TaskNode taskNode = (TaskNode)task;
        Double io_charge = (taskNode.getReadOps() + taskNode.getWriteOps()) * (taskNode.getChargingPlan().getChargePerIO());
        Double cpu_charge = task.getLength() * (taskNode.getChargingPlan().getChargePerMi());
        Double mem_charge = (taskNode.getMemoryNeed()/1024)*(taskNode.getChargingPlan().getChargePerMemoryMB());
        return io_charge+cpu_charge+mem_charge;
    }
    // Formula (6,7) from the paper
    public static double calculateWaitingDelay(Task task, ComputingNode edgeServer) {
        double c = 1.0; // size of base computational task
        double v = 1.0; // Value of 1MB task for the base value
        double charge = getRevenue(task);
        double alpha =  (task.getLength()/c+charge/v-1)*(task.getLength()/c+charge/v-2)+charge/v;  // formula from the paper (6)
        double gamma = 1;   // task arrival rate, set as constant
        double mu = edgeServer.getMipsPerCore();    // service rate = processing capacity of the edge server
        double  averageWaitingTime = (1/mu) / (1 - (gamma/mu));

        if (Double.isNaN(averageWaitingTime) || Double.isInfinite(averageWaitingTime)){
            return 0;
        } else {
            return  alpha* averageWaitingTime;
        }
    }

    // Formula (8) from the paper
//    public static double calculateProcessingDelay(Task task, ComputingNode edgeServer) {
//        return  task.getLength()/edgeServer.getMipsPerCore();
//    }

    // Formula (11) from the paper
//    public static double calculateTotalCompletionTime(List<Whale> population, List<Task> tasks,
//                                                      List<ComputingNode> edgeServers, List<Channel> channels) {
//
//        double totalCompletionTime = 0;
//        for (Whale whale : population){
//            for(int i=0; i< whale.position.length; i++){
//                int edgeServerIndex = whale.position[i];
//                totalCompletionTime +=  calculateCompletionTime(tasks.get(i),edgeServers.get(edgeServerIndex));
//            }
//        }
//        return totalCompletionTime;
//
//    }
    // Formula (18,19) from the paper
    public static double calculateActualPayment(double completionTime, Task task){
        double idealCompletionTime = task.getMaxLatency()*0.8;
        double charge = getRevenue(task);
        if (completionTime < idealCompletionTime){
            return charge;
        } else if(completionTime > idealCompletionTime && completionTime < task.getMaxLatency()){
            double ratio  =   (task.getMaxLatency()-completionTime)/(task.getMaxLatency() - idealCompletionTime);
            return ratio*charge;
        }
        else{
            return 0;
        }
    }

    // Formula (20) from the paper
//    public static double calculateRevenue(List<Whale> population, List<Task> tasks, List<ComputingNode> edgeServers,
//                                          List<Channel> channels){
//        double totalRevenue = 0;
//        for (Whale whale : population) {
//            for (int i=0; i < whale.position.length; i++) {
//                int edgeServerIndex = whale.position[i] -1;
//                double completionTime = calculateCompletionTime(tasks.get(i),edgeServers.get(edgeServerIndex));
//                totalRevenue += calculateActualPayment(completionTime, tasks.get(i));
//            }
//
//        }
//
//        return totalRevenue;
//
//    }

    // Formula (12,13,14,15,16,17)  // Simplified as there is no load balancing
//    public static double calculateTotalEnergyConsumption(List<ComputingNode> edgeServers){
//        double totalEnergyConsumption = 0;
//        for(ComputingNode edgeServer: edgeServers){
//            double pk = 1 + (edgeServer.maxPower  - 1) * 0.5; // simplified utilization, and pIde =1 and pmax = maxPower
//            totalEnergyConsumption += pk;
//        }
//        return totalEnergyConsumption;
//    }


}