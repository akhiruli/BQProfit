package com.mechalikh.pureedgesim.datacentersmanager;

import com.mechalikh.pureedgesim.locationmanager.Location;
import com.mechalikh.pureedgesim.network.InfrastructureGraph;
import com.mechalikh.pureedgesim.network.NetworkLink;
import com.mechalikh.pureedgesim.network.NetworkLinkMan;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;

import java.util.ArrayList;
import java.util.List;

public class DataCenter {
    public List<ComputingNode> nodeList;
    private String name;
    private Location location;
    private SimulationManager simManager;
    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    public DataCenter(String dcname, SimulationManager simulationManager){
        this.name = dcname;
        simManager = simulationManager;
        nodeList = new ArrayList<>();
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void createTopology() {
        if(nodeList.size() > 1) { //for single node we dont need intra topology
            InfrastructureGraph infrastructureTopology = new InfrastructureGraph();
            int i = 0;
            int j = 1;
            while(j <nodeList.size()){ //creating ring topology
                ComputingNode nodeFrom = nodeList.get(i);
                ComputingNode nodeTo = nodeList.get(j);
                infrastructureTopology.addLink(new NetworkLinkMan(nodeFrom, nodeTo, simManager, NetworkLink.NetworkLinkTypes.MAN));
                infrastructureTopology.addLink(new NetworkLinkMan(nodeTo, nodeFrom, simManager, NetworkLink.NetworkLinkTypes.MAN));

                i++;
                j++;
            }

            if(j > 2){ //link from last node to first node
                ComputingNode nodeFrom = nodeList.get(0);
                ComputingNode nodeTo = nodeList.get(nodeList.size() - 1);
                infrastructureTopology.addLink(new NetworkLinkMan(nodeFrom, nodeTo, simManager, NetworkLink.NetworkLinkTypes.MAN));
                infrastructureTopology.addLink(new NetworkLinkMan(nodeTo, nodeFrom, simManager, NetworkLink.NetworkLinkTypes.MAN));
            }
            //0->1->2->0
        }
    }
}
