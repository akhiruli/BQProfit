package BQProfit.IMOBWO.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import org.apache.commons.math3.special.Gamma;
import BQProfit.Helper;
import BQProfit.dag.TaskNode;

public class IMOBWO {
    private int populationSize = 40;
    private int maxIterations = 100;
    private int variableDimension; // Number of tasks
    private List<Task> tasks; // List of all tasks
    private List<ComputingNode> edgeServers; // List of all edge servers
    private Random random = new Random();
    public Whale bestWhale;
    private List<Whale> population;
    SimulationManager simManager;
    int edgeDCSize;

    public IMOBWO (List<Task> tasks, SimulationManager simulationManager){
        this.tasks = tasks;
        this.simManager = simulationManager;
        this.variableDimension = tasks.size();
        edgeDCSize = simManager.getDataCentersManager().getEdgeDatacenterList().get(0).nodeList.size();
        this.population = initializePopulation();
    }

    // Algorithm 6 : Initialization of the population
    public List<Whale> initializePopulation(){
        List<Whale> population = new ArrayList<>();
        for(int i = 0; i < populationSize; i++){
            Whale whale = new Whale(i,variableDimension);
            for(int j=0; j < variableDimension;j++){
                whale.position[j] = (int) Helper.getRandomInteger(0, edgeDCSize-1);// Number of edge server starts at 1
                //whale.position[j] = random.nextInt(simulationManager.getDataCentersManager().getEdgeDatacenterList().size() + 2) - 1; // Number of edge server starts at 1
            }
            population.add(whale);
        }
        return population;
    }

    // Step 1 of Algorithm 6 and 1 for the fitness calculations
    public double[] computeFitness(Whale whale) {
        double totalCompletionTime = 0;
        double totalRevenue =0;

        for(int i=0; i < whale.position.length; i++){
            //int edgeServerIndex = whale.position[i] - 1;
            int edgeServerIndex = 0;
            ComputingNode computingNode = null;
            if(whale.position[i] < -1){
                edgeServerIndex = (int) Helper.getRandomInteger(-1, 0);
            } else{
                edgeServerIndex = whale.position[i];
            }
            if(edgeServerIndex == -1){
                //Assuming single DC in cloud
                computingNode = simManager.getDataCentersManager().getCloudDatacentersList().get(0).nodeList.get(0);
            } else if(edgeServerIndex == 0){
                computingNode = this.tasks.get(i).getEdgeDevice();
            } else{
                //Single DC
                int index = edgeServerIndex >= edgeDCSize? (int) Helper.getRandomInteger(0, edgeDCSize - 1) : edgeServerIndex-1;
                computingNode = simManager.getDataCentersManager().getEdgeDatacenterList().get(0).nodeList.get(index);
            }
            totalCompletionTime +=  MathFunctions.calculateCompletionTime(this.tasks.get(i), computingNode);
            if(edgeServerIndex == -1 || edgeServerIndex > 0) {
                totalRevenue += MathFunctions.calculateActualPayment(totalCompletionTime, tasks.get(i));
            }
        }

        double[] fitness = {totalCompletionTime, -totalRevenue};
        return fitness;
    }

    // Step 2 of Algorithm 6
    public void computeBestWhaleInPopulation() {
        for(Whale whale : population){
            whale.setFitness(computeFitness(whale));
        }

        if(bestWhale == null){
            bestWhale = population.get(0).clone();
        }

        for (Whale whale : population) {
            if (whale.getFitness()[1] > bestWhale.getFitness()[1]){
                bestWhale = whale.clone();
            }
        }
    }

    // Method 4.2 of the paper
    private Whale findBestWhaleInGroup() {
        List<Whale> optimalSolutions = findNonDominatedSolutions();
        Whale bestWhale = null;

        if (!optimalSolutions.isEmpty()) {
            bestWhale =  computeCentroid(optimalSolutions);
            double minDistance = Double.POSITIVE_INFINITY;
            for(Whale whale: optimalSolutions){
                double distance = computeEuclideanDistance(bestWhale,whale);
                if(distance < minDistance){
                    bestWhale = whale;
                    minDistance = distance;
                }
            }

        }

        return bestWhale;
    }

    // Step 4 of Algorithm 6: Non-dominated sorting strategy
    private List<Whale> findNonDominatedSolutions() {
        List<Whale> nonDominatedSolutions = new ArrayList<>();
        for(Whale whale1: population){
            boolean dominated = false;
            for(Whale whale2 : population){
                if(whale1.id != whale2.id){
                    if(isDominating(whale2,whale1)){
                        dominated = true;
                        break;
                    }
                }
            }
            if(!dominated){
                nonDominatedSolutions.add(whale1);
            }
        }

        return nonDominatedSolutions;
    }

    private boolean isDominating(Whale whale1, Whale whale2) {

        return whale1.getFitness()[0] <= whale2.getFitness()[0] && whale1.getFitness()[1]>= whale2.getFitness()[1];

    }
    // Step 4 of Algorithm 6: Compute centroid of the optimal set
    private Whale computeCentroid(List<Whale> optimalSolutions) {
        int k = optimalSolutions.size();
        double f1Centroid =0;
        double f2Centroid =0;
        for(Whale whale : optimalSolutions){
            f1Centroid += whale.getFitness()[0];
            f2Centroid += whale.getFitness()[1];
        }
        Whale centroid = new Whale(-1,variableDimension);
        centroid.setFitness(new double[]{f1Centroid / k, f2Centroid / k});
        return centroid;
    }

    //Step 4 of Algorithm 6: compute euclidean distance of a whale to the centroid
    private double computeEuclideanDistance(Whale centroid, Whale whale){
        double f1Distance = (whale.getFitness()[0] - centroid.getFitness()[0]);
        double f2Distance = (whale.getFitness()[1] - centroid.getFitness()[1]);
        return Math.sqrt(Math.pow(f1Distance,2) + Math.pow(f2Distance,2));
    }

    // Step 5 - 17 of Algorithm 6: updating the positions

    private void updatePositions(){
        double tMax = maxIterations;
        double t = 1; // Time step is set as one.
        double balanceFactor = calculateBalanceFactor(t,tMax);
        double fallFactor = calculateFallFactor(t,tMax);
        for (Whale whale : population){
            if(balanceFactor > 0.5){
                updateSwimmingPosition(whale);
            } else if (balanceFactor < 0.5){
                updatePredationPosition(whale);
            }
            if (balanceFactor < fallFactor){
                updateMigrationPosition(whale);
            }
        }
        mutationOperation();
    }

    // Formula (28)
    private double calculateBalanceFactor(double t, double tMax) {
        double bo = random.nextDouble();
        return bo*(1 - t/tMax);
    }

    // Formula (29)
    private double calculateFallFactor(double t, double tMax) {
        return 0.1 * 0.05 * t/tMax;
    }

    // Algorithm 2, and (30,31)
    private void updateSwimmingPosition(Whale whale){
        for(int i = 0; i < variableDimension; i++){
            double r1 = random.nextDouble();
            double r2 = random.nextDouble();
            if(i % 2 == 1){
                int pj = random.nextInt(variableDimension);
                double  xnew =  whale.position[i] +  (bestWhale.position[pj] - whale.position[i])*(1+r1)*Math.sin(2*Math.PI*r2);
                whale.position[i] = (int) Math.round(xnew);
            } else{
                int pj = random.nextInt(variableDimension);
                double  xnew =  whale.position[i] +  (bestWhale.position[pj] - whale.position[i])*(1+r1)*Math.cos(2*Math.PI*r2);
                whale.position[i] = (int) Math.round(xnew);
            }
        }
    }

    // Algorithm 3, and formula (32,33,34)
    private void updatePredationPosition(Whale whale) {
        double r3 = random.nextDouble();
        double r4 = random.nextDouble();
        Whale randomWhale = population.get(random.nextInt(populationSize));
        double randomJumpIntensity =  2 * r3 * (1 - ((double)1/maxIterations));
        double levyFlight = 0.05 * generateLevyFlight();
        for(int i = 0; i < variableDimension; i++){
            double xNew  = r3 * bestWhale.position[i] - r4* whale.position[i] + randomJumpIntensity * levyFlight*(bestWhale.position[i] - randomWhale.position[i]);
            whale.position[i] = (int) Math.round(xNew);
        }

    }

    // formula (35)
    private double generateLevyFlight() {
        double beta = 1.5;
        double v = random.nextGaussian();
        double u = random.nextGaussian();
        double numerator = u * Math.pow(Math.abs(Gamma.gamma(1+beta)*Math.sin(Math.PI*beta/2)
                /(Math.abs(Gamma.gamma((1+beta)/2)) * beta * Math.pow(2,(beta-1)/2)
        )), 1/beta);
        double denominator = Math.pow(Math.abs(v),(1/beta));
        return numerator/denominator;
    }

    // Algorithm 4, and formula (36,37,38)
    private void updateMigrationPosition(Whale whale) {
        double r5 = random.nextDouble();
        double r6 = random.nextDouble();
        double r7 = random.nextDouble();
        double cStep = ( 1 - 1 )*Math.exp(-(2 * calculateFallFactor(1, maxIterations) * populationSize) /maxIterations ); // Using t = 1 to keep it simple
        for(int i = 0; i < variableDimension; i++){
            double  xNew = r5 * bestWhale.position[i] - r6 * whale.position[i] + r7* cStep;
            whale.position[i] = (int) Math.round(xNew);
        }
    }
    // Algorithm 5 and formulas (39,40,41)
    private void mutationOperation() {
        double pt = 0.001*( (double)(maxIterations-1)/maxIterations); // simplified from formula 39
        double e = random.nextDouble();

        if( e < pt){
            int numberOfWhalesToMutate = (int) Math.round((0.5 * populationSize) * Math.pow(1-((double)1/maxIterations),1/0.5)); // Using t = 1 to keep it simple
            for(int i = 0; i < numberOfWhalesToMutate; i++){
                int index = random.nextInt(populationSize);
                Whale whale = population.get(index);
                for (int j=0; j < variableDimension;j++){
                    int xNew = (int) Math.round(0.001 + random.nextDouble()* (1- 0.001)); // simplified from formula 41, using a constant for us and lb
                    whale.position[j] = xNew;
                }
            }
        }
    }

    public void run(Double budget) {
        for (int t = 1; t <= maxIterations; t++) {
            computeBestWhaleInPopulation();
            updatePositions();
            // find the best whale again after the positions have been updated
            Whale currentBestWhale = findBestWhaleInGroup();
            if(currentBestWhale.getFitness()[1] > bestWhale.getFitness()[1]){
                bestWhale = currentBestWhale;
            }
        }
        Double serviceProviderCost = 0.0;
        for(int i=0; i<tasks.size();i++){
            TaskNode taskNode = (TaskNode) tasks.get(i);
            int[] position = bestWhale.getPosition();
            if(position[i] <= -1){
                taskNode.setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
            } else {
                serviceProviderCost += calculateServiceProviderCharge(taskNode);
                if(serviceProviderCost <= budget) {
                    if (position[i] == 0) {
                        taskNode.setTaskDecision(TaskNode.TaskDecision.CLOUD);
                    } else {
                        taskNode.setSelectedComputingNode(simManager.getDataCentersManager().getEdgeDatacenterList().get(0).nodeList.get(position[i] - 1));
                        taskNode.setTaskDecision(TaskNode.TaskDecision.MEC_ONLY);
                    }
                } else{
                    taskNode.setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
                }
            }
        }
    }

    private double calculateServiceProviderCharge(TaskNode taskNode){
        double extra_cost = 0;
        double cpu_cost = taskNode.getLength() * taskNode.getChargingPlan().getChargePerMi();
        double memory_cost = (taskNode.getMemoryNeed()/1024)*taskNode.getChargingPlan().getChargePerMemoryMB();
        double io_cost = (taskNode.getReadOps() + taskNode.getWriteOps())*taskNode.getChargingPlan().getChargePerIO();
        Double total_cost = cpu_cost + memory_cost + io_cost;

        if(taskNode.getDeadlineType().equals("hard")){
            extra_cost += total_cost*taskNode.getChargingPlan().getHard_deadline_surcharge();
        }

        if(taskNode.isPeakTimeOffload()){
            extra_cost += total_cost*taskNode.getChargingPlan().getHigh_demand_surcharge();
        }

        return total_cost + extra_cost;
    }
}
