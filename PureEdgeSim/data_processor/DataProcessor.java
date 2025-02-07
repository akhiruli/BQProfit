package data_processor;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;
import com.mechalikh.pureedgesim.datacentersmanager.DataCentersManager;
import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import BQProfit.ChargingPlan;
import BQProfit.Helper;
import BQProfit.IMOBWO.algorithm.IMOBWO;
import BQProfit.ScientificWorkflowParser;
import BQProfit.bqprofit.algorithm.Gene;
import BQProfit.bqprofit.algorithm.RaGeneticAlgorithm;
import BQProfit.bqprofit.algorithm.TaskCost;
import BQProfit.bqprofit.algorithm.mkl;
import BQProfit.dag.TaskNode;
import BQProfit.simulation_graph.Cost;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;

public class DataProcessor {
    public static int MAX_ITR = 10;
    public static Map<Integer, Job> scheduledJob;
    public static Map<Integer, Job> jobsMap;
    public static Map<Integer, Map<Integer, TaskNode>> tasksMap;
    private List<? extends ComputingNode> devices;
    SimulationManager simManager;
    private Integer ueDeviceIndex;
    private ChargingPlan chargingPlan;
    private ScientificWorkflowParser scientificWorkflowParser;
    private Cost costPlan;
    //static final String task_dataset_path = "/Users/akhirul.islam/edgeSim_intellij_prob2/jobs_test";
    //static final String task_dataset_path = "/Users/akhirul.islam/edgeSim_intellij_prob2/jobs/deadline_type_test";
    static final String task_dataset_path = "/Users/akhirul.islam/edgeSim_intellij_prob2/jobs";
    static Integer totalCpuIntensiveTask = 0;
    public DataProcessor(SimulationManager simulationManager,
                         List<? extends ComputingNode> devicesList){
        tasksMap = new HashMap<>();
        scheduledJob = new HashMap<>();
        jobsMap = new HashMap<>();
        simManager = simulationManager;
        devices = devicesList;
        ueDeviceIndex = 0;
        this.loadChargingPlan();
        this.loadCostPlan();
        //scientificWorkflowParser = new ScientificWorkflowParser(tasksMap, chargingPlan, jobsMap);
        //scientificWorkflowParser.loadTasks();
        this.loadTasks();
        //this.assignDeadlineType();
        this.allocateBudgetToJob();
        this.assignUEDevice();
        this.assignPeakDemandTask(0);
        this.assignDeadlineTypeNew(0);
        this.assignOffloadingDecision();
        //this.assignPeakDemandTask(50);
    }

    private void loadChargingPlan() {
        chargingPlan = new ChargingPlan();
        JSONParser parser = new JSONParser();
        try{
            Object obj = parser.parse(new FileReader("PureEdgeSim/BQProfit/settings/charging_plan.json"));
            JSONObject jsonObject = (JSONObject)obj;
            Double cpu = (Double) jsonObject.get("cpu");
            Double memory = (Double) jsonObject.get("memory");
            Double io = (Double) jsonObject.get("io");
            Double penalty = (Double) jsonObject.get("penalty");
            Double hard_deadline_surcharge = (Double) jsonObject.get("hard_deadline_surcharge");
            Double high_demand_surcharge = (Double)jsonObject.get("high_demand_surcharge");
            chargingPlan.setChargePerIO(io);
            chargingPlan.setChargePerMi(cpu);
            chargingPlan.setChargePerMemoryMB(memory);
            chargingPlan.setPenalty(penalty);
            chargingPlan.setHigh_demand_surcharge(high_demand_surcharge);
            chargingPlan.setHard_deadline_surcharge(hard_deadline_surcharge);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void loadCostPlan(){
        costPlan = new Cost();
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("PureEdgeSim/BQProfit/settings/cost_plan.json"));
            JSONObject jsonObject = (JSONObject) obj;
            Double very_high_cpu = (Double) jsonObject.get("very_high_cpu");
            Double high_cpu = (Double) jsonObject.get("high_cpu");
            Double medium_cpu = (Double) jsonObject.get("medium_cpu");
            Double low_cpu = (Double) jsonObject.get("low_cpu");
            Double very_high_memory = (Double) jsonObject.get("very_high_memory");
            Double high_memory = (Double) jsonObject.get("high_memory");
            Double medium_memory = (Double) jsonObject.get("medium_memory");
            Double low_memory = (Double) jsonObject.get("low_memory");

            Double very_high_io = (Double) jsonObject.get("very_high_io");
            Double high_io = (Double) jsonObject.get("high_io");
            Double medium_io = (Double) jsonObject.get("medium_io");
            Double low_io = (Double) jsonObject.get("low_io");

            costPlan.setVery_high_costPerMi(very_high_cpu);
            costPlan.setHigh_costPerMi(high_cpu);
            costPlan.setMedium_costPerMi(medium_cpu);
            costPlan.setLow_costPerMi(low_cpu);

            costPlan.setVery_high_costPerMemoryMB(very_high_memory);
            costPlan.setHigh_costPerMemoryMB(high_memory);
            costPlan.setMedium_costPerMemoryMB(medium_memory);
            costPlan.setLow_costPerMemoryMB(low_memory);

            costPlan.setVery_high_costPerIO(very_high_io);
            costPlan.setHigh_costPerIO(high_io);
            costPlan.setMedium_costPerIO(medium_io);
            costPlan.setLow_costPerIO(low_io);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    public Map<Integer, Map<Integer, TaskNode>> getTasksMap() {
        return tasksMap;
    }

    public List<Task> getTaskList(){
        List<Task> taskList = new ArrayList<>();
        Integer job_count = 0;
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            if(!isValidDag(job)){
                System.out.println("DAG with ap_id: " + entry.getKey() + " is not valid");
                continue;
            }

            job_count++;
            List<TaskNode> tempTaskList = new ArrayList<>();
            for (Map.Entry<Integer, TaskNode> task : job.entrySet()) {
                task.getValue().setContainerSize(2500);
                taskList.add(task.getValue());
                tempTaskList.add(task.getValue());
            }

            scheduledJob.put(entry.getKey(), jobsMap.get(entry.getKey()));
            if(job_count >= MAX_ITR)
               break;
        }

        return taskList;
    }

    private void assignPeakDemandTask() {
        //We are assuming 10% of the tasks are offloaded in the high demand time
        Integer total_open_tasks = 0; //not including start and end task of the DAG
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            for (Map.Entry<Integer, TaskNode> task : job.entrySet()) {
                if(!task.getValue().isStartTask() && !task.getValue().isEndTask()) {
                    total_open_tasks++;
                }
            }
        }

        Integer high_demand_tasks = (int) (total_open_tasks*0.1);
        Integer counter = 0;
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            for (Map.Entry<Integer, TaskNode> task : job.entrySet()) {
                TaskNode taskNode = task.getValue();
                if(!taskNode.isStartTask() && !taskNode.isEndTask()) {
                    if(counter%10 == 0){
                        taskNode.setPeakTimeOffload(true);
                        counter++;
                    }
                }
            }
        }
    }

    private void assignPeakDemandTask(int peakPct) {
        //We are assuming 10% of the tasks are offloaded in the high demand time
        Integer total_open_tasks = 0; //not including start and end task of the DAG
        List<TaskNode> tempTaskList = new ArrayList<>();
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            for (Map.Entry<Integer, TaskNode> task : job.entrySet()) {
                if(!task.getValue().isStartTask() && !task.getValue().isEndTask()) {
                    total_open_tasks++;
                }

                tempTaskList.add(task.getValue());
            }
        }

        Integer high_demand_tasks = (int) (total_open_tasks*(peakPct/100.0));
        if(high_demand_tasks == 0)
            return;

        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            for (Map.Entry<Integer, TaskNode> task : job.entrySet()) {
                if(!task.getValue().isStartTask() && !task.getValue().isEndTask() && task.getValue().getTaskDecision() == TaskNode.TaskDecision.MEC_ONLY) {
                    if(high_demand_tasks > 0){
                        task.getValue().setPeakTimeOffload(true);
                        high_demand_tasks--;
                    }
                }
            }
        }

//        Collections.sort(tempTaskList, getCompByCPUNeed());
//        for(TaskNode taskNode : tempTaskList){
//            if(!taskNode.isStartTask() && !taskNode.isEndTask() && taskNode.getTaskDecision() == TaskNode.TaskDecision.MEC_ONLY) {
//                if(high_demand_tasks > 0){
//                    taskNode.setPeakTimeOffload(true);
//                    high_demand_tasks--;
//                }
//            }
//        }

        /*List<Integer> index_list = Helper.getListofRandomNumberInRange(1, tempTaskList.size()-1, high_demand_tasks+2);
        for(Integer index: index_list){
            tempTaskList.get(index).setPeakTimeOffload(true);
        }*/

        /*Integer counter = 0;

        for(TaskNode taskNode : tempTaskList){
            if(!taskNode.isStartTask() && !taskNode.isEndTask()) {

            }
        }
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            for (Map.Entry<Integer, TaskNode> task : job.entrySet()) {
                TaskNode taskNode = task.getValue();
                if(!taskNode.isStartTask() && !taskNode.isEndTask()) {
                    if(counter%10 == 0){
                        taskNode.setPeakTimeOffload(true);
                        counter++;
                    }
                }
            }
        }*/
    }
    public static Comparator<TaskNode> getCompByCPUNeed()
    {
        Comparator comp = new Comparator<TaskNode>(){
            @Override
            public int compare(TaskNode t1, TaskNode t2)
            {
                if (t1.getLength() == t2.getLength())
                    return 0;
                else if(t1.getLength() < t2.getLength())
                    return 1;
                else
                    return -1;
            }
        };
        return comp;
    }

    void printDag(Map<Integer, TaskNode> job){
        for(Map.Entry<Integer, TaskNode> task :  job.entrySet()) {
            TaskNode taskNode = task.getValue();
            String pred_str = "";
            String succ_str = "";
            for(TaskNode t : taskNode.successors){
                if(succ_str.isEmpty()){
                    succ_str += t.getId();
                } else{
                    succ_str += "->" + t.getId();
                }
            }

            for(TaskNode t : taskNode.predecessors){
                if(pred_str.isEmpty()){
                    pred_str += t.getId();
                } else{
                    pred_str += "->" + t.getId();
                }
            }

            System.out.println("Task: " + taskNode.getId() + " predecessors: " + pred_str + " Successors: " + succ_str);
        }
    }

    boolean isValidDag(Map<Integer, TaskNode> job){
        boolean result = true;
        for(Map.Entry<Integer, TaskNode> task :  job.entrySet()) {
            TaskNode taskNode = task.getValue();
            if (taskNode.isStartTask()) {
                if (taskNode.predecessors.size() != 0 || taskNode.successors.size() == 0) {
                    result = false;
                    break;
                }
            } else if (taskNode.isEndTask()) {
                if (taskNode.predecessors.size() == 0 || taskNode.successors.size() != 0) {
                    result = false;
                    break;
                }
            } else {
                if (taskNode.predecessors.size() == 0 || taskNode.successors.size() == 0) {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }
    void assignUEDevice(){
        if(devices.size() == 0) {
            System.out.println("No UE devices available");
            return;
        }
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            for(Map.Entry<Integer, TaskNode> task :  job.entrySet()){
                TaskNode taskNode = task.getValue();
                taskNode.setEdgeDevice(devices.get(ueDeviceIndex));
            }

            ueDeviceIndex++;
            if(ueDeviceIndex >= devices.size()){
                ueDeviceIndex = 0;
            }
        }
    }

    public static List<Path> listFiles(Path path) throws IOException {

        List<Path> result;
        try (Stream<Path> walk = Files.walk(path)) {
            result = walk.filter(Files::isRegularFile)
                    .collect(Collectors.toList());
        }
        return result;

    }
    public void loadTasks() {
        try {
            Path path = Paths.get(task_dataset_path);
            List<Path> paths = listFiles(path);
            Double deadline_max = Double.MIN_VALUE;
            Double deadline_min = Double.MAX_VALUE;
            Long cpu_min = Long.MAX_VALUE;
            Long cpu_max = Long.MIN_VALUE;
            Long io_read_min = Long.MAX_VALUE;
            Long io_read_max = Long.MIN_VALUE;
            Long io_write_min = Long.MAX_VALUE;
            Long io_write_max = Long.MIN_VALUE;
            int task_max = MIN_VALUE;
            int task_min = MAX_VALUE;
            for (Path file_path : paths) {
                File file = file_path.toFile();
                BufferedReader reader;
                long lineCount = 0;
                FileInputStream inputStream = null;
                Scanner sc = null;
                Integer taskCount = 0;
                Map<Integer, List<Integer>> dependency_list = new HashMap<>();
                Integer jobId = Integer.valueOf(String.valueOf(file_path.getFileName()).split("_")[1]);
                Map<Integer, TaskNode> tMap = null;
                Job job = new Job();
                try {
                    inputStream = new FileInputStream(String.valueOf(file_path));
                    sc = new Scanner(inputStream);
                    while (sc.hasNextLine()) {
                        String line = sc.nextLine();
                        String fields[] = line.split(" ");
                        String dependencies_str = null;

                        Integer task_id = Integer.valueOf(fields[1]);
                        TaskNode taskNode = null;
                        ++taskCount;
                        if (fields[3].equals("ROOT")) {
                            taskNode = new TaskNode(task_id, 1);
                            dependencies_str = fields[2];
                            taskNode.setMemoryNeed(1.0);
                            taskNode.setFileSize(1);
                            taskNode.setReadOps(1);
                            taskNode.setWriteOps(1);
                            taskNode.setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
                            taskNode.setStartTask(true);
                            taskNode.setDeadlineType("soft");
                            taskNode.setMaxLatency(1);
                            taskNode.setJob(job);
                        } else if (fields[3].equals("END")) {
                            taskNode = new TaskNode(task_id, 1);
                            taskNode.setMemoryNeed(1.0);
                            taskNode.setFileSize(1);
                            taskNode.setReadOps(1);
                            taskNode.setWriteOps(1);
                            taskNode.setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
                            taskNode.setEndTask(true);
                            taskNode.setDeadlineType("soft");

                            taskNode.setMaxLatency(1);
                            taskNode.setJob(job);
                        } else { //Computation tasks
                            dependencies_str = fields[2];
                            Long cpu = Long.valueOf(Integer.valueOf(fields[5]));
                            Double memory = Double.valueOf(fields[6]);
                            Long read_io = Long.valueOf(Integer.valueOf(fields[7]));
                            Long write_io = Long.valueOf(Integer.valueOf(fields[8]));
                            Double deadline = Double.valueOf(fields[9]);
                            Integer deadline_type = Integer.valueOf(fields[10]);
                            taskNode = new TaskNode(task_id, cpu);
                            taskNode.setMemoryNeed(memory);
                            taskNode.setReadOps(read_io);
                            taskNode.setWriteOps(write_io);
                            taskNode.setMaxLatency(deadline);
                            if(deadline_type > 0){
                                taskNode.setDeadlineType("hard");
                            } else {
                                taskNode.setDeadlineType("soft");
                            }
                            taskNode.setStorageNeed((double) Helper.getRandomInteger(500, 1000));
                            taskNode.setFileSize(Helper.getRandomInteger(8000000, 400000000)).setOutputSize(Helper.getRandomInteger(8000000, 400000000));
                            taskNode.setTaskDecision(TaskNode.TaskDecision.OPEN);
                            taskNode.setJob(job);

                            if(cpu < cpu_min){
                                cpu_min = cpu;
                            }

                            if(cpu > cpu_max){
                                cpu_max = cpu;
                            }

                            if(read_io < io_read_min){
                                io_read_min = read_io;
                            }

                            if(read_io > io_read_max){
                                io_read_max = read_io;
                            }

                            if(write_io < io_write_min){
                                io_write_min = write_io;
                            }

                            if(write_io > io_read_max){
                                io_write_max = write_io;
                            }

                            if(deadline < deadline_min){
                                deadline_min = deadline;
                            }

                            if(deadline_max < deadline){
                                deadline_max = deadline;
                            }
                        }

                        taskNode.setApplicationID(jobId);
                        taskNode.setId(task_id);
                        taskNode.setContainerSize(25000);
                        taskNode.setChargingPlan(this.chargingPlan);

                        if (dependencies_str != null) {
                            String dependencies[] = dependencies_str.split(",");
                            for (int i = 0; i < dependencies.length; i++) {
                                if (!dependency_list.containsKey(task_id)) {
                                    List<Integer> d_tasks = new ArrayList<>();
                                    d_tasks.add(Integer.valueOf(dependencies[i]));
                                    dependency_list.put(task_id, d_tasks);
                                } else {
                                    dependency_list.get(task_id).add(Integer.valueOf(dependencies[i]));
                                }
                            }
                        }
                        if (tasksMap.containsKey(jobId)) {
                            tasksMap.get(jobId).put(task_id, taskNode);
                        } else {
                            tMap = new HashMap<>();
                            tMap.put(task_id, taskNode);
                            tasksMap.put(jobId, tMap);
                        }
                    }
                    sc.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }

                for (Map.Entry<Integer, List<Integer>> task_dep_pair : dependency_list.entrySet()) {
                    Integer parent_id = task_dep_pair.getKey();
                    List<Integer> children = task_dep_pair.getValue();
                    for (Integer child : children) {
                        TaskNode childNode = tasksMap.get(jobId).get(child);
                        TaskNode parentNode = tasksMap.get(jobId).get(parent_id);
                        tasksMap.get(jobId).get(parent_id).successors.add(childNode);
                        tasksMap.get(jobId).get(child).predecessors.add(parentNode);
                    }
                }

                job.setJobID(jobId);
                job.setTaskMap(tMap);

                job.setBudget(this.getBudget(tMap));
                jobsMap.put(jobId, job);
                //scheduledJob.put(jobId, job);
                if(taskCount > task_max){
                    task_max = taskCount;
                }

                if(taskCount < task_min){
                    task_min = taskCount;
                }

                taskCount = 0;
            }
            //System.out.println("min Task: "+task_min+" max task: "+task_max+" cpu min: "+ cpu_min+" cpu max: "+cpu_max);
            //System.out.println("IO read min: "+io_read_min+" IO read max: "+io_read_max+"IO write min: "+io_write_min+" IO write max: "+io_write_max);
            //System.out.println("Deadline min: "+deadline_min+" Deadline max: "+deadline_max);
        }catch (IOException io){
            io.printStackTrace();
        }
    }

    private Double getBudget(Map<Integer, TaskNode> tMap) {
        Double budget = 0.0;
        for(Map.Entry<Integer, TaskNode> task : tMap.entrySet()){
            TaskNode taskNode = task.getValue();
            budget += taskNode.getLength()*chargingPlan.getChargePerMi() +
                    (taskNode.getMemoryNeed()/1024)*chargingPlan.getChargePerMemoryMB() +
                    (taskNode.getWriteOps() + taskNode.getReadOps())*chargingPlan.getChargePerIO();
        }
        return budget*Helper.getRandomDouble(0.6, 0.8); //assuming 70% budget
    }

    private void assignDeadlineType() {
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            Integer soft_pct = Math.toIntExact(Helper.getRandomInteger(55, 75));
            Integer soft_tasks = (int) (job.size()*(soft_pct/100.0));
            List<Integer> index_list = Helper.getListofRandomNumberInRange(1, job.size()-1, 5);
            for(Integer index: index_list){
                entry.getValue().get(index).setDeadlineType("soft");
            }

            for (Map.Entry<Integer, TaskNode> task : job.entrySet()) {
                if(task.getValue().isStartTask() || task.getValue().isEndTask()){
                    task.getValue().setDeadlineType("soft");
                } else if (task.getValue().getDeadlineType() == null) {
                    task.getValue().setDeadlineType("hard");
                }
            }

        }
    }


    void randomRRScheduleDecision(List<TaskNode> tempTaskList){
        for(TaskNode taskNode : tempTaskList){
            if(taskNode.getTaskType() == TaskNode.TaskType.NORMAL){
                Integer decision = Math.toIntExact(Helper.getRandomInteger(0, 1));
                if(decision == 0)
                    taskNode.setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
                else
                    taskNode.setTaskDecision(TaskNode.TaskDecision.MEC_ONLY);
            } else{
                taskNode.setTaskDecision(TaskNode.TaskDecision.MEC_ONLY);
            }
        }
    }

    double getMECComputingTime(TaskNode taskNode){ //only for multi-user scheduling algo
        double cpuTime = taskNode.getLength()/40000;

        double io_time = taskNode.getStorageNeed() * 60 / (100 * 1000) //READ operation, 60% read
                + taskNode.getStorageNeed() * 40 / (100 * 500); //WRITE operation, 40% write;
        double mem_tr_time = taskNode.getMemoryNeed() / 300000;

        return cpuTime + io_time + mem_tr_time;
    }

    void allocateBudgetToJob(){
        Double min_budget = Double.MAX_VALUE;
        Double max_budget = Double.MIN_VALUE;
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            Double total_charge = 0.0;
            for(Map.Entry<Integer, TaskNode> job_entry : job.entrySet()){
                TaskNode taskNode = job_entry.getValue();
                total_charge += chargingPlan.getChargePerMi()*taskNode.getLength() +
                        chargingPlan.getChargePerMemoryMB()*(taskNode.getMemoryNeed()/1024) +
                        chargingPlan.getChargePerIO()*(taskNode.getReadOps() + taskNode.getWriteOps());
            }

            Integer budget_pct = Math.toIntExact(Helper.getRandomInteger(50, 60));
            for(Map.Entry<Integer, TaskNode> job_entry : job.entrySet()){
                TaskNode taskNode = job_entry.getValue();
                taskNode.getJob().setBudget(budget_pct*total_charge/100);
                if(min_budget > budget_pct*total_charge/100){
                    min_budget = budget_pct*total_charge/100;
                }

                if(max_budget < budget_pct*total_charge/100){
                    max_budget = budget_pct*total_charge/100;
                }
            }
            //scheduledJob.get(entry.getKey()).setBudget((budget_pct/100)*total_charge);
        }

        //System.out.println("Min budget: " + min_budget+ "Max Budget: "+max_budget);
    }

    void assignOffloadingDecision(){
        if(simManager.getScenario().getStringOrchAlgorithm().equals("BQPROFIT_ALGO")) {
            this.assignDecForBqprofit();
        } else if(simManager.getScenario().getStringOrchAlgorithm().equals("AUCTION_BASED")){
            this.assignDecForAuctionAlgo();
        } else if(simManager.getScenario().getStringOrchAlgorithm().equals("MKL_RR")){
            assignDecForMKLRR();
        } else if(simManager.getScenario().getStringOrchAlgorithm().equals("IMOBWO")){
            assignDecForImobwo();
        }
    }

    void assignDecForBqprofit(){
        int count = 0;
        for (Map.Entry<Integer, Job> jobentry : jobsMap.entrySet()) {
            Job job = jobentry.getValue();
            mkl partiton_algo = new mkl(job.getTaskMap(), simManager, job.getBudget());
            partiton_algo.partition();

            List<TaskCost> locallist = partiton_algo.getLocalList();
            List<TaskCost> remotelist = partiton_algo.getRemotelist();
            List<TaskNode> newremotelist = new ArrayList<>();
            for (TaskCost taskCost : locallist) {
                taskCost.getTaskNode().setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
            }

            for (TaskCost taskCost : remotelist) {
                if (taskCost.getTaskNode().isEndTask() || taskCost.getTaskNode().isStartTask()) {
                    taskCost.getTaskNode().setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
                } else {
                    taskCost.getTaskNode().setTaskDecision(TaskNode.TaskDecision.MEC_ONLY);
                }

                newremotelist.add(taskCost.getTaskNode());
            }
            System.out.println(locallist.size() + " : " + remotelist.size());
            //
            RaGeneticAlgorithm geneticAlgorithm = new RaGeneticAlgorithm(simManager, newremotelist);
            List<Gene> fittestIndividual = geneticAlgorithm.mapTaskToServer();
            Integer taskIndex = 0;
            for (Gene gene : fittestIndividual) {
                DataCentersManager manager = simManager.getDataCentersManager();
                if (gene.getDcIndex() >= manager.getEdgeDatacenterList().size()) {
                    remotelist.get(taskIndex).getTaskNode().setSelectedComputingNode(manager.getCloudDatacentersList().get(0).nodeList.get(0));
                } else {
                    remotelist.get(taskIndex).getTaskNode().setTaskDecision(TaskNode.TaskDecision.MEC_ONLY);
                    remotelist.get(taskIndex).getTaskNode().setSelectedComputingNode(manager.getEdgeDatacenterList().get(gene.getDcIndex()).nodeList.get(gene.getServerIndex()));
                }

                ++taskIndex;
                if (taskIndex >= remotelist.size()) {
                    break;
                }
            }
            count++;
            if(count >= MAX_ITR)
                break;
        }
    }

    void assignDecForAuctionAlgo(){
        double queue_time = 0.0;
        for (Map.Entry<Integer, Job> jobentry : jobsMap.entrySet()) {
            Job job = jobentry.getValue();
            if(job != null) {
                Map.Entry<Integer, TaskNode> entry = job.getTaskMap().entrySet().iterator().next();
                double energy_remain = entry.getValue().getEdgeDevice().getEnergyModel().getBatteryLevel();
                for (Map.Entry<Integer, TaskNode> task : job.taskMap.entrySet()) {
                    TaskNode taskNode = task.getValue();
                    double latency = (taskNode.getLength() / taskNode.getEdgeDevice().getMipsPerCore()) + queue_time;
                    //double energy_need = (0.01 * (taskNode.getLength() * taskNode.getLength()) / (taskNode.getEdgeDevice().getMipsPerCore() * taskNode.getEdgeDevice().getMipsPerCore()) * latency) / 3600;
                    double energy_need = (0.01 * (taskNode.getLength() * taskNode.getLength()) / (taskNode.getEdgeDevice().getMipsPerCore() * taskNode.getEdgeDevice().getMipsPerCore()) * latency);
                    if(latency < taskNode.getMaxLatency() && energy_need < energy_remain) {
                        taskNode.setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
                        energy_remain -= energy_need;
                        queue_time += latency*1.5;
                    }
                }
            }
        }
    }

    void assignDecForMKLRR(){
        for (Map.Entry<Integer, Job> jobentry : jobsMap.entrySet()) {
            Job job = jobentry.getValue();
            mkl partiton_algo = new mkl(job.getTaskMap(), simManager, job.getBudget());
            partiton_algo.partition();

            List<TaskCost> locallist = partiton_algo.getLocalList();
            List<TaskCost> remotelist = partiton_algo.getRemotelist();
            List<TaskNode> newremotelist = new ArrayList<>();
            for (TaskCost taskCost : locallist) {
                taskCost.getTaskNode().setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
            }

            System.out.println(locallist.size()+" : "+remotelist.size());

            for (TaskCost taskCost : remotelist) {
                if (taskCost.getTaskNode().isEndTask() || taskCost.getTaskNode().isStartTask()) {
                    taskCost.getTaskNode().setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
                } else {
                    taskCost.getTaskNode().setTaskDecision(TaskNode.TaskDecision.MEC_ONLY);
                }

                newremotelist.add(taskCost.getTaskNode());
            }
        }
    }

    void assignDecForImobwo(){
        for (Map.Entry<Integer, Job> jobentry : jobsMap.entrySet()) {
            Job job = jobentry.getValue();
            List<Task> taskList = new ArrayList<>();
            for (Map.Entry<Integer, TaskNode> task : job.taskMap.entrySet()) {
                TaskNode taskNode = task.getValue();
                taskList.add(taskNode);
            }

            IMOBWO imobwo = new IMOBWO(taskList, simManager);
            imobwo.run(job.getBudget());
        }
    }

    void assignDeadlineTypeNew(int hardPct){
        int total = 0;
        int h_count = 0;
        int s_count = 0;
        if(hardPct == 0)
            return;
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            for (Map.Entry<Integer, TaskNode> task : job.entrySet()) {
                task.getValue().setDeadlineType(null);
            }
        }
        for(Map.Entry<Integer, Map<Integer, TaskNode>> entry : tasksMap.entrySet()){
            Map<Integer, TaskNode> job = entry.getValue();
            Integer hard_tasks = (int) (job.size()*(hardPct/100.0));
            List<Integer> index_list = Helper.getListofRandomNumberInRange(1, job.size()-1, hard_tasks+2);
            for(Integer index: index_list){
                job.get(index).setDeadlineType("hard");
                h_count++;
            }

            for (Map.Entry<Integer, TaskNode> task : job.entrySet()) {
                ++total;
                if(task.getValue().isStartTask() || task.getValue().isEndTask()){
                    if (task.getValue().getDeadlineType() != null && task.getValue().getDeadlineType().equals("hard")){
                        h_count--;
                    }
                    task.getValue().setDeadlineType("soft");
                } else if (task.getValue().getDeadlineType() == null) {
                    task.getValue().setDeadlineType("soft");
                    s_count++;
                }
            }
        }

        System.out.println("Total: "+total+" Hard: "+h_count+" Soft: "+s_count);
    }
}

