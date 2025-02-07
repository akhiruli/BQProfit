package BQProfit;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.datacentersmanager.DataCenter;
import com.mechalikh.pureedgesim.scenariomanager.SimulationParameters;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import BQProfit.dag.TaskNode;

import java.util.*;

public class Helper {
    public static double calculateExecutionTime(ComputingNode computingNode, Task task){
        double mem_tr_time = 0.0;
        double io_time = 0;
        double cpu_time = 0;

        cpu_time  = task.getLength() / computingNode.getMipsPerCore();

        if(computingNode.getDataBusBandwidth() > 0) {
            mem_tr_time = task.getMemoryNeed() / computingNode.getDataBusBandwidth();
        }
        if(task.getStorageType().equals("SSD") && computingNode.isSsdEnabled()){
            io_time = task.getReadOps() / computingNode.getSsdReadBw() //READ operation, 60% read
                    + task.getWriteOps() / computingNode.getSsdWriteBw(); //WRITE operation, 40% write;;
        } else {
            if(computingNode.getReadBandwidth() > 0 && computingNode.getWriteBandwidth() > 0) {
                io_time = task.getReadOps() / computingNode.getReadBandwidth() //READ operation, 60% read
                        + task.getWriteOps()/ computingNode.getWriteBandwidth(); //WRITE operation, 40% write;
            }
        }

        double total_latency = 0;
        total_latency = cpu_time + io_time + mem_tr_time;


        return total_latency;
    }


    //This includes min and max
    public static long getRandomInteger(Integer min, Integer max){
        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
        //return (int) ((Math.random() * (max - min)) + min);
    }

    public static double getRandomDouble(double min, double max){
        Random rand = new Random();
        return rand.nextDouble() * (max - min) + min;
    }

    public static List<Integer> getListofRandomNumberInRange(Integer min, Integer max, Integer n){
        Set<Integer> hash_set= new HashSet<Integer>();
        while(hash_set.size() < n){
            long number = getRandomInteger(min, max);
            if(!hash_set.contains(number)){
                hash_set.add((int) number);
            }
        }
        List<Integer> number_list = new ArrayList<>(hash_set);
        return number_list;
    }

    public static double calculateExecutionTime_new(ComputingNode computingNode, Task task){
        double io_time = 0;
        double cpu_time = 0;
        double mem_tr_time = 0;

        cpu_time  = task.getLength() / computingNode.getMipsPerCore();
        TaskNode taskNode = (TaskNode) task;
        if(computingNode.getReadBandwidth() > 0){
            io_time += taskNode.getReadOps()/computingNode.getReadBandwidth();
        }

        if(computingNode.getWriteBandwidth() > 0){
            io_time += taskNode.getWriteOps()/computingNode.getWriteBandwidth();
        }

        if(computingNode.getDataBusBandwidth() > 0) {
            mem_tr_time = task.getMemoryNeed() / computingNode.getDataBusBandwidth();
        }

        double total_latency = cpu_time + io_time + mem_tr_time;
        //System.out.println(cpu_time + " : " +  io_time + " : " + total_latency);
        return total_latency;
    }

    public static double getDataRate(double bw){
        float leftLimit = 0.2F;
        float rightLimit = 0.7F;
        float generatedFloat = leftLimit + new Random().nextFloat() * (rightLimit - leftLimit);
        return 2*bw*generatedFloat;
    }

    public static double calculatePropagationDelay(double distance){
        return distance*10/300000000;
    }

    public static double getManEdgeTransmissionLatency(double bits){
        double dataRate = Helper.getDataRate(SimulationParameters.MAN_BANDWIDTH_BITS_PER_SECOND);
        return bits/dataRate;
    }

    public static double getManCloudTransmissionLatency(double bits){
        double dataRate = Helper.getDataRate(SimulationParameters.WAN_BANDWIDTH_BITS_PER_SECOND);
        return bits/dataRate;
    }

    public static double getWirelessTransmissionLatency(double bits){
        double dataRate = Helper.getDataRate(SimulationParameters.WIFI_BANDWIDTH_BITS_PER_SECOND);
        return bits/dataRate;
    }

    public static Double calculateDistance(DataCenter dc1, DataCenter dc2){
        double x1 = dc1.getLocation().getXPos();
        double y1 = dc1.getLocation().getYPos();
        double x2 = dc2.getLocation().getXPos();
        double y2 = dc2.getLocation().getYPos();
        double distance = Math.sqrt(Math.pow((x2-x1), 2) + Math.pow((y2-y1), 2));
        return distance;
    }
}
