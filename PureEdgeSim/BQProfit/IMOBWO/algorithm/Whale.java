package BQProfit.IMOBWO.algorithm;

import java.util.Random;

public class Whale {
    int id;
    int[] position; // 0 for local, -1 for cloud and otherwise id of the edge server
    double[] fitness;
    Random random = new Random();

    public Whale(int id, int taskCount) {
        this.id = id;
        this.position = new int[taskCount];
        this.fitness = new double[2];
    }

    public int[] getPosition() {
        return position;
    }

    public void setPosition(int[] position) {
        this.position = position;
    }
    public double[] getFitness() {
        return fitness;
    }
    public void setFitness(double[] fitness) {
        this.fitness = fitness;
    }

    // Method to clone a whale (Important for iterations)
    @Override
    public Whale clone() {
        Whale clonedWhale = new Whale(this.id,this.position.length);
        clonedWhale.setPosition(this.position.clone());
        clonedWhale.setFitness(this.fitness.clone());
        return clonedWhale;
    }
}