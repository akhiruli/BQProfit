package BQProfit.auction_based;

public class BidingValue {
    private Double chargePerMi;

    private Double chargePerMemoryMB; //memory
    private Double chargePerIO; //based on number of IO operations
    public BidingValue(Double chPerMi, Double chPerIO, Double chPerMemoryMB){
        chargePerMi = chPerMi;
        chargePerIO = chPerIO;
        chargePerMemoryMB = chPerMemoryMB;
    }
    public Double getChargePerMi() {
        return chargePerMi;
    }

    public Double getChargePerMemoryMB() {
        return chargePerMemoryMB;
    }

    public Double getChargePerIO() {
        return chargePerIO;
    }
}
