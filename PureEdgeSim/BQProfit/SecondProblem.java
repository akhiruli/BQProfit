package BQProfit;

import com.mechalikh.pureedgesim.simulationmanager.Simulation;
import BQProfit.dag.DependentTaskGenerator;

public class SecondProblem {
   private static String settingsPath;
   private static String outputPath;
   private String algo = "bqprofit";
   //private String algo = "auction";
   //private String algo = "mkl-rr";
   //private String algo = "imobwo";
    public SecondProblem(){
        Simulation sim = new Simulation();
        if(algo.equals("bqprofit")) {
            settingsPath = "PureEdgeSim/BQProfit/bqprofit/settings/";
            outputPath = "PureEdgeSim/BQProfit/bqprofit/output/";
        } else if (algo.equals("auction")) {
            settingsPath = "PureEdgeSim/BQProfit/auction_based/settings/";
            outputPath = "PureEdgeSim/BQProfit/auction_based/output/";
        } else if (algo.equals("mkl-rr")) {
            settingsPath = "PureEdgeSim/BQProfit/mkl_rr/settings/";
            outputPath = "PureEdgeSim/BQProfit/mkl_rr/output/";
        } else if (algo.equals("imobwo")){
            settingsPath = "PureEdgeSim/BQProfit/IMOBWO/settings/";
            outputPath = "PureEdgeSim/BQProfit/IMOBWO/output/";
        } else{
            System.out.println("Please use correct strategy");
        }
        sim.setCustomOutputFolder(outputPath);
        sim.setCustomSettingsFolder(settingsPath);
        sim.setCustomTaskGenerator(DependentTaskGenerator.class);
        sim.setCustomEdgeOrchestrator(CustomOrchestrator.class);
        sim.launchSimulation();;
    }

    public static void main(String args[]){
        new SecondProblem();
    }
}


/*
Raft consensus algorithm
* @article{ongaro2015raft,
  title={The raft consensus algorithm},
  author={Ongaro, Diego and Ousterhout, John},
  journal={Lecture Notes CS},
  volume={190},
  pages={2022},
  year={2015}
}
*


@article{huang2019performance,
  title={Performance analysis of the raft consensus algorithm for private blockchains},
  author={Huang, Dongyan and Ma, Xiaoli and Zhang, Shengli},
  journal={IEEE Transactions on Systems, Man, and Cybernetics: Systems},
  volume={50},
  number={1},
  pages={172--181},
  year={2019},
  publisher={IEEE}
}
* */