package data_processor;

import BQProfit.dag.TaskNode;

import java.util.Map;

public class Job {
    private boolean status;
    private Integer JobID;
    private Double budget;
    Map<Integer, TaskNode> taskMap;
    public Map<Integer, TaskNode> getTaskMap() {
        return taskMap;
    }

    public void setTaskMap(Map<Integer, TaskNode> taskMap) {
        this.taskMap = taskMap;
    }
    public Double getBudget() {
        return budget;
    }
    public void setBudget(Double budget) {
        this.budget = budget;
    }
    public Job(){
        status = false;
    }
    public boolean isStatus() {
        return status;
    }
    public void setStatus(boolean status) {
        this.status = status;
    }
    public Integer getJobID() {
        return JobID;
    }
    public void setJobID(Integer jobID) {
        JobID = jobID;
    }
    public void decrBudget(Double charge){
        budget -= charge;
    }
}
