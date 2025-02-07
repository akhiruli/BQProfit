package BQProfit.auction_based;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.datacentersmanager.DataCenter;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import BQProfit.bqprofit.algorithm.utility;

import java.util.Collections;
import java.util.List;

public class Auction {
    SimulationManager simulationManager;
    public Auction(SimulationManager simManager){
        simulationManager = simManager;
    }

    public void startAuction(List<Bid> bids){
        Collections.sort(bids);
        for (Bid bid : bids){
            if(bid.getTotalBid() < bid.getTaskNode().getJob().getBudget()) {
                ComputingNode computingNode = findSuitableServer(bid);
                if (computingNode != null) {
                    bid.getTaskNode().setBidingValue(bid.getBidingValue());
                    bid.getTaskNode().setBudget(bid.getTotalBid());
                    bid.getTaskNode().setSelectedComputingNode(computingNode);
                    bid.getTaskNode().getJob().decrBudget(bid.getTotalBid());
                    break;
                }
            }
        }
    }

    private ComputingNode findSuitableServer(Bid bid){
        DataCenter cloudDc = simulationManager.getDataCentersManager().getCloudDatacentersList().get(0);
        List<DataCenter> edgeDcList = simulationManager.getDataCentersManager().getEdgeDatacenterList();
        for(DataCenter dataCenter : edgeDcList){
            for(ComputingNode comNode : dataCenter.nodeList) {
                Double latency = utility.calculateLatency(bid.getTaskNode(), comNode, edgeDcList.get(0), false);
                if(latency < bid.getTaskNode().getMaxLatency()){
                    return comNode;
                }
            }
        }

        Double latency = utility.calculateLatency(bid.getTaskNode(), cloudDc.nodeList.get(0), edgeDcList.get(0), true);
        if(latency < bid.getTaskNode().getMaxLatency()){
            return cloudDc.nodeList.get(0);
        }
        return null;
    }
}
