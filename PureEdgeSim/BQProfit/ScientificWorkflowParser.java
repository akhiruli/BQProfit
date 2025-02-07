package BQProfit;

import data_processor.Job;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import BQProfit.dag.TaskNode;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ScientificWorkflowParser {
    public static String workflow_path = "/Users/akhirul.islam/Downloads/ScientificWFGen/WorkFlowBenchmark/Montage/";
    public static String output_dir = "/Users/akhirul.islam/edgeSim_intellij_prob2/ScientificWorkflow/";
    //public static String output_dir = "/Users/akhirul.islam/edgeSim_intellij_prob2/ScientificWorkflow1/";
    Map<Integer, Map<Integer, TaskNode>> tasksMap;
    Map<Integer, Job> jobsMap;
    ChargingPlan chargingPlan;

    static String arr[] = {/*"Montage_25.xml", "Montage_50.xml", "Montage_100.xml",*/ "Montage_1000.xml"};
    public ScientificWorkflowParser(Map<Integer, Map<Integer, TaskNode>> tMap,
                                    ChargingPlan cPlan, Map<Integer, Job> jMap){
        tasksMap = tMap;
        chargingPlan = cPlan;
        jobsMap = jMap;
    }
    public void loadScientificWorkflow(){
        Integer mindata = 2048;
        Integer maxdata = 11264;
        try {
            Path path = Paths.get(workflow_path);
            List<Path> paths = listFiles(path);
            for (Path file_path : paths) {
                File xmlFile = new File(String.valueOf(file_path));
                String outFileName = String.valueOf(file_path.getFileName());
                if(!outFileName.endsWith(".xml")){
                    continue;
                }

                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(xmlFile);

                NodeList job_nodes = doc.getElementsByTagName("job");
                for (int i = 0; i < job_nodes.getLength(); i++) {
                    Node job = job_nodes.item(i);
                    if (job.getNodeType() == Node.ELEMENT_NODE) {
                        Integer datasize = Math.toIntExact(Helper.getRandomInteger(mindata, maxdata)) / 1024; //in KB
                        Integer deadline_type = Math.toIntExact(Helper.getRandomInteger(0, 1));
                        Double op = Helper.getRandomDouble(2.0, 512.0);
                        //Double cost = op * Math.pow(datasize, 2.0);
                        op = Helper.getRandomDouble(2.0, 512.0);
                        Double cpu = op * Math.pow(datasize, 2.0);
                        op = Helper.getRandomDouble(2.0, 512.0);
                        Double memory = op * Math.pow(datasize, 2.0);
                        op = Helper.getRandomDouble(2.0, 512.0);
                        Double rd_io = op * Math.pow(datasize, 2.0);
                        op = Helper.getRandomDouble(2.0, 512.0);
                        Double wr_io = op * Math.pow(datasize, 2.0);

                        Element task_length_node = doc.createElement("task_length");
                        task_length_node.appendChild(doc.createTextNode(String.valueOf(cpu)));
                        job.appendChild(task_length_node);

                        Element memory_node = doc.createElement("memory");
                        memory_node.appendChild(doc.createTextNode(String.valueOf(memory)));
                        job.appendChild(memory_node);

                        Element rd_io_node = doc.createElement("rd_io");
                        rd_io_node.appendChild(doc.createTextNode(String.valueOf(rd_io)));
                        job.appendChild(rd_io_node);

                        Element wr_io_node = doc.createElement("wr_io");
                        wr_io_node.appendChild(doc.createTextNode(String.valueOf(wr_io)));
                        job.appendChild(wr_io_node);

                        Element deadline_node = doc.createElement("deadline");
                        deadline_node.appendChild(doc.createTextNode(String.valueOf(get_deadline(cpu, rd_io + wr_io, memory))));
                        job.appendChild(deadline_node);

                        Element deadline_type_node = doc.createElement("deadline_type");
                        if (deadline_type == 0) {
                            deadline_type_node.appendChild(doc.createTextNode("soft"));
                        } else {
                            deadline_type_node.appendChild(doc.createTextNode("hard"));
                        }
                        job.appendChild(deadline_type_node);
                    }
                }


                writeXml(doc, outFileName);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    static double get_deadline(double cpu_len, double io, double memory){
        double deadline = 0;
        double upper_mips = 40000;
        double lower_mips = 1000;
        //double lower_io = 1000;
        double upper_io = 10000;
        //double mem_bw = 300000;

        double high_latency = cpu_len/lower_mips;
        //double low_latency = cpu_len/upper_mips + io/upper_io + memory/300000;
        deadline = Helper.getRandomDouble(30*high_latency, 30*high_latency + high_latency*20);
        return deadline;
    }

    public static List<Path> listFiles(Path path) throws IOException {

        List<Path> result;
        try (Stream<Path> walk = Files.walk(path)) {
            result = walk.filter(Files::isRegularFile)
                    .collect(Collectors.toList());
        }
        return result;

    }

    private static void writeXml(Document doc, String fileName) {
        try {
            FileOutputStream output =
                    new FileOutputStream(output_dir + fileName);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();

            // The default add many empty new line, not sure why?
            // https://mkyong.com/java/pretty-print-xml-with-java-dom-and-xslt/
            // Transformer transformer = transformerFactory.newTransformer();

            // add a xslt to remove the extra newlines
            Transformer transformer = transformerFactory.newTransformer();

            // pretty print
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.STANDALONE, "no");

            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(output);

            transformer.transform(source, result);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void loadTasks(){
        try {
            Path path = Paths.get(output_dir);
            List<Path> paths = listFiles(path);
            Integer jobId = 1;
            for (Path file_path : paths) {
                //System.out.println(file_path);
                String outFileName = String.valueOf(file_path.getFileName());
                if(outFileName.endsWith(".swp"))
                    continue;

//                if(!isTargetFile(outFileName))
//                    continue;

                File xmlFile = new File(String.valueOf(file_path));
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(xmlFile);
                NodeList job_nodes = doc.getElementsByTagName("job");
                Map<Integer, TaskNode> tMap = null;
                Job job = new Job();
                job.setJobID(jobId);
                System.out.println(job_nodes.getLength());
                for (int i = 0; i < job_nodes.getLength(); i++) {
                    Element jobElement = (Element) job_nodes.item(i);
                    Integer id = Integer.valueOf(jobElement.getAttribute("id").substring(2))+1;

                    Element taskLengthElement = (Element) jobElement.getElementsByTagName("task_length").item(0);
                    Integer taskLength = (int) Math.ceil(Double.valueOf(taskLengthElement.getTextContent()));

                    TaskNode taskNode =new TaskNode(id, taskLength);

                    Element memoryElement = (Element) jobElement.getElementsByTagName("memory").item(0);
                    Double memory = Double.valueOf(memoryElement.getTextContent());

                    Element rdIoElement = (Element) jobElement.getElementsByTagName("rd_io").item(0);
                    Long rdIo = (long) Math.abs(Double.valueOf(rdIoElement.getTextContent()));

                    Element wrIoElement = (Element) jobElement.getElementsByTagName("wr_io").item(0);
                    Long wrIo = (long) Math.abs(Double.valueOf(wrIoElement.getTextContent()));

                    Element deadlineElement = (Element) jobElement.getElementsByTagName("deadline").item(0);
                    Double deadline = Double.valueOf(deadlineElement.getTextContent());

                    Element deadlineTypeElement = (Element) jobElement.getElementsByTagName("deadline_type").item(0);
                    String deadlineType = deadlineTypeElement.getTextContent();

                    taskNode.setMemoryNeed(memory);
                    taskNode.setReadOps(rdIo);
                    taskNode.setWriteOps(wrIo);
                    taskNode.setMaxLatency(deadline);
                    taskNode.setDeadlineType(deadlineType);

                    taskNode.setStorageNeed((double) Helper.getRandomInteger(500, 1000));
                    taskNode.setFileSize(Helper.getRandomInteger(8000000, 400000000)).setOutputSize(Helper.getRandomInteger(8000000, 400000000));
                    taskNode.setTaskDecision(TaskNode.TaskDecision.OPEN);
                    taskNode.setJob(job);

                    taskNode.setApplicationID(jobId);
                    taskNode.setId(id);
                    taskNode.setContainerSize(25000);
                    taskNode.setChargingPlan(this.chargingPlan);

                    if (tasksMap.containsKey(jobId)) {
                        tasksMap.get(jobId).put(id, taskNode);
                    } else {
                        tMap = new HashMap<>();
                        tMap.put(id, taskNode);
                        tasksMap.put(jobId, tMap);
                    }
                }

                NodeList child_nodes = doc.getElementsByTagName("child");
                for(int j=0; j<child_nodes.getLength();j++){
                    Element childElement = (Element) child_nodes.item(j);
                    Integer child_id = Integer.valueOf(childElement.getAttribute("ref").substring(2))+1;
                    TaskNode childTaskNode = tMap.get(child_id);
                    if(childTaskNode == null){
                        System.out.println("Got task node as NULL that is not valid");
                        throw new RuntimeException();
                    }
                    NodeList parents_node = childElement.getElementsByTagName("parent");
                    for(int k=0; k<parents_node.getLength(); k++){
                        Element parentElement = (Element) parents_node.item(k);
                        Integer parent_id = Integer.valueOf(parentElement.getAttribute("ref").substring(2))+1;
                        TaskNode parentTaskNode = tMap.get(parent_id);
                        if(parentTaskNode == null){
                            System.out.println(childTaskNode.getId());
                        }
                        childTaskNode.predecessors.add(parentTaskNode);
                        parentTaskNode.successors.add(childTaskNode);
                    }
                }

                setStartEndTaskFlag(tMap, job);

                job.setJobID(jobId);
                job.setTaskMap(tMap);
                job.setBudget(this.getBudget(tMap));
                jobsMap.put(jobId, job);
                jobId++;
            }
        }catch (Exception e){
           e.printStackTrace();
        }
    }

    void setStartEndTaskFlag(Map<Integer, TaskNode> tMap, Job job){
        int startCount = 0;
        int endCount = 0;
        int max_id = Integer.MAX_VALUE;
        for(Map.Entry<Integer, TaskNode> task : tMap.entrySet()){
            TaskNode taskNode = task.getValue();
            if(taskNode.predecessors.size() ==0){
                taskNode.setStartTask(true);
                startCount++;
            } else if(taskNode.successors.size() == 0){
                taskNode.setEndTask(true);
                endCount++;
            }

            if(taskNode.getId() > max_id){
                max_id = taskNode.getId();
            }
        }

        if(startCount > 1){
            TaskNode dummyTaskNode = new TaskNode(0, 1);
            dummyTaskNode.setMemoryNeed(1.0);
            dummyTaskNode.setFileSize(1);
            dummyTaskNode.setReadOps(1);
            dummyTaskNode.setWriteOps(1);
            dummyTaskNode.setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
            dummyTaskNode.setStartTask(true);
            dummyTaskNode.setDeadlineType("soft");
            dummyTaskNode.setMaxLatency(1);
            dummyTaskNode.setJob(job);
            dummyTaskNode.setChargingPlan(chargingPlan);
            for(Map.Entry<Integer, TaskNode> task : tMap.entrySet()){
                TaskNode taskNode = task.getValue();
                if(taskNode.isStartTask()){
                    dummyTaskNode.successors.add(taskNode);
                    taskNode.predecessors.add(dummyTaskNode);
                    taskNode.setStartTask(false);
                }
            }

            dummyTaskNode.setStartTask(true);
            tMap.put(0, dummyTaskNode);
        }

        if(endCount >1){
            TaskNode dummyTaskNode = new TaskNode(max_id+1, 1);
            dummyTaskNode.setMemoryNeed(1.0);
            dummyTaskNode.setFileSize(1);
            dummyTaskNode.setReadOps(1);
            dummyTaskNode.setWriteOps(1);
            dummyTaskNode.setTaskDecision(TaskNode.TaskDecision.UE_ONLY);
            dummyTaskNode.setStartTask(true);
            dummyTaskNode.setDeadlineType("soft");
            dummyTaskNode.setMaxLatency(1);
            dummyTaskNode.setJob(job);
            dummyTaskNode.setChargingPlan(chargingPlan);
            for(Map.Entry<Integer, TaskNode> task : tMap.entrySet()){
                TaskNode taskNode = task.getValue();
                if(taskNode.isStartTask()){
                    dummyTaskNode.predecessors.add(taskNode);
                    taskNode.successors.add(dummyTaskNode);
                    taskNode.setEndTask(false);
                }
            }

            dummyTaskNode.setEndTask(true);
            tMap.put(max_id+1, dummyTaskNode);
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

    boolean isTargetFile(String filname){
        for(int i =0; i< arr.length;i++){
            if(filname.equals(arr[i])){
                return true;
            }
        }

        return false;
    }
}
