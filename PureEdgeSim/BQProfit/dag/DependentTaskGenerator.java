package BQProfit.dag;

import com.mechalikh.pureedgesim.simulationmanager.SimulationManager;
import com.mechalikh.pureedgesim.taskgenerator.Task;
import com.mechalikh.pureedgesim.taskgenerator.TaskGenerator;
import data_processor.DataProcessor;

import java.util.List;

public class DependentTaskGenerator extends TaskGenerator {
    private double simulationTime;
    public DependentTaskGenerator(SimulationManager simulationManager) {
        super(simulationManager);
    }
    @Override
    public List<Task> generate() {
        DataProcessor dataProcessor = new DataProcessor(getSimulationManager(), devicesList);
        return dataProcessor.getTaskList();
    }
}
