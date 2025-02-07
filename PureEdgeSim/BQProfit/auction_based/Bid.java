package BQProfit.auction_based;

import BQProfit.dag.TaskNode;

public class Bid implements Comparable<Bid>{
    TaskNode taskNode;

    BidingValue bidingValue;
    private Double totalBid;
    public Bid(TaskNode tNode, BidingValue bidValue){
        taskNode = tNode;
        bidingValue = bidValue;

        totalBid = (taskNode.getMemoryNeed()/1024)*bidValue.getChargePerMemoryMB() +
                taskNode.getLength() * bidingValue.getChargePerMi() +
                (taskNode.getReadOps() + taskNode.getWriteOps()) * bidingValue.getChargePerIO();
    }

    public void resetBid(BidingValue bidValue){
        bidingValue = bidValue;
        totalBid = (taskNode.getMemoryNeed()/1024)*bidValue.getChargePerMemoryMB() +
                taskNode.getLength() * bidingValue.getChargePerMi() +
                (taskNode.getReadOps() + taskNode.getWriteOps()) * bidingValue.getChargePerIO();
    }
    public Double getTotalBid() {
        return totalBid;
    }
    public TaskNode getTaskNode() {
        return taskNode;
    }
    public BidingValue getBidingValue() {
        return bidingValue;
    }

    public void setBidingValue(BidingValue bidingValue) {
        this.bidingValue = bidingValue;
    }

    @Override
    public int compareTo(Bid other) {
        return -Double.compare(this.totalBid, other.totalBid);
    }

    @Override
    public String toString() {
        return "";
    }
}
