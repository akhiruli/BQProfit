package BQProfit.block_chain;

import BQProfit.dag.TaskNode;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Block {
    private int index;
    private List<TaskNode> tasks;
    private String previousHash;
    private String hash;

    public Block(int index, List<TaskNode> tasks, String previousHash){
        this.index = index;
        this.tasks = tasks;
        this.previousHash = previousHash;
        this.hash = calculateHash();
    }

    public String getPreviousHash(){
        return previousHash;
    }

    private String calculateHash() {
        try{
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            StringBuilder data = new StringBuilder();
            data.append(index).append(tasks).append(previousHash);
            byte[] hashBytes = digest.digest(data.toString().getBytes());

            // Convert hash bytes to hexadecimal string
            StringBuilder hexString = new StringBuilder();
            for (byte hashByte : hashBytes) {
                String hex = Integer.toHexString(0xff & hashByte);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }

            return hexString.toString();
        } catch (NoSuchAlgorithmException e){
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String toString() {
        return "Block{" +
                "index=" + index +
                ", tasks=" + tasks +
                ", previousHash='" + previousHash + '\'' +
                ", hash='" + hash + '\'' +
                '}';
    }
}
