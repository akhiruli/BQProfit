package BQProfit.block_chain;
import BQProfit.dag.TaskNode;

import java.util.ArrayList;
import java.util.List;


public class Blockchain {
    private List<Block> chain;
    private List<TaskNode> pendingTasks;
    public static int TASKS_PER_BLOCK = 2;
    public Blockchain() {
        this.chain = new ArrayList<>();
        this.pendingTasks = new ArrayList<>();
        // Create the genesis block
        createBlock("0");
    }

    private void createBlock(String previousHash) {
        Block block = new Block(chain.size() + 1, pendingTasks, previousHash);
        pendingTasks = new ArrayList<>();
        chain.add(block);
        //System.out.println("Creating blocks: " + block.toString());
    }

    public void addTask(TaskNode task) {
        pendingTasks.add(task);
        if(pendingTasks.size() >= TASKS_PER_BLOCK){
            Block lastBlock = this.getLastBlock();
            this.createBlock(lastBlock.getPreviousHash());
        }
    }

    public Block getLastBlock() {
        return chain.get(chain.size() - 1);
    }

    // Override toString() to print blockchain information
    @Override
    public String toString() {
        return "Blockchain{" +
                "chain=" + chain +
                '}';
    }
}
