package BQProfit.dag;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import data_processor.Job;
import BQProfit.ChargingPlan;
import BQProfit.auction_based.BidingValue;
import BQProfit.simulation_graph.Cost;

import java.util.ArrayList;
import java.util.List;


public class TaskNode extends Task{
    public enum TaskType {
        NONE,
        NORMAL,
        IO_INTENSIVE,
        CPU_INTENSIVE,
        MEM_INTENSIVE
    };

    public enum TaskDecision{
        UE_ONLY,
        MEC_ONLY,
        OPEN,
        CLOUD
    };
    public List<TaskNode>   predecessors;
    public List<TaskNode>   successors;
    List<Integer> predecessorsId;
    List<Integer> successorsId;
    private boolean taskDone;
    private Integer level;
    private boolean startTask;
    private boolean endTask;
    private TaskType taskType;
    private TaskDecision taskDecision;
    private ChargingPlan chargingPlan;
    private Double budget;
    private boolean isPeakTimeOffload;

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    private Job job;

    private BidingValue bidingValue;

    public ComputingNode getSelectedComputingNode() {
        return selectedComputingNode;
    }

    public void setSelectedComputingNode(ComputingNode selectedComputingNode) {
        this.selectedComputingNode = selectedComputingNode;
    }

    private ComputingNode selectedComputingNode;
    public Double getBudget() {
        return budget;
    }
    public void setBudget(Double budget) {
        this.budget = budget;
    }
    public ChargingPlan getChargingPlan() {
        return chargingPlan;
    }
    public void setChargingPlan(ChargingPlan chargingPlan) {
        this.chargingPlan = chargingPlan;
    }
    public TaskDecision getTaskDecision() {
        return taskDecision;
    }
    public void setTaskDecision(TaskDecision taskDecision) {
        this.taskDecision = taskDecision;
    }
    public TaskNode(int id, long length){
        super(id, length);
        predecessors = new ArrayList<>();
        successors = new ArrayList<>();
        successorsId = new ArrayList<>();
        predecessorsId = new ArrayList<>();
        taskDone = false;
        level = 0;
        startTask = false;
        endTask = false;
        taskType = TaskType.NORMAL;
        taskDecision = TaskDecision.OPEN;
    }
    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }
    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public boolean isStartTask() {
        return startTask;
    }

    public void setStartTask(boolean startTask) {
        this.startTask = startTask;
    }

    public boolean isEndTask() {
        return endTask;
    }

    public void setEndTask(boolean endTask) {
        this.endTask = endTask;
    }

    public boolean isTaskDone() {
        return taskDone;
    }
    public void setTaskDone(boolean taskDone) {
        this.taskDone = taskDone;
    }
    public List<Integer> getPredecessorsId() {
        return predecessorsId;
    }

    public void setPredecessorsId(List<Integer> predecessorsId) {
        this.predecessorsId = predecessorsId;
    }

    public List<Integer> getSuccessorsId() {
        return successorsId;
    }

    public void setSuccessorsId(List<Integer> successorsId) {
        this.successorsId = successorsId;
    }
    public boolean isPeakTimeOffload() {
        return isPeakTimeOffload;
    }

    public void setPeakTimeOffload(boolean peakTimeOffload) {
        isPeakTimeOffload = peakTimeOffload;
    }
    public BidingValue getBidingValue() {
        return bidingValue;
    }

    public void setBidingValue(BidingValue bidingValue) {
        this.bidingValue = bidingValue;
    }
}
