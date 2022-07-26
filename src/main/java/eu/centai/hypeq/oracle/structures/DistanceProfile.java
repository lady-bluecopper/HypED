package eu.centai.hypeq.oracle.structures;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import org.javatuples.Triplet;

/**
 *
 * @author giulia
 */
public class DistanceProfile {
    
    private final int p;
    private final int q;
    // for each s, store lower-bound, upper-bound, and (approximate) s-distance
    // in the case of exact profile, lower=upper=distance
    Map<Integer, Triplet<Double, Double, Double>> distances;
    
    /**
     * Initialize distance profile.
     * The parameters p and q can be two vertices, two edges, or one vertex + 
     * one edge.
     * 
     * @param p first id
     * @param q first id
     */
    public DistanceProfile(int p, int q) {
        this.p = p;
        this.q = q;
        this.distances = Maps.newHashMap();
    }
    
    /**
     * Create distance profile from a map of distances.
     * The parameters p and q can be two vertices, two edges, or one vertex + one edge.
     * 
     * @param p first id
     * @param q first id
     * @param distances lower-bound, upper-bound, and estimated distance, for each s
     */
    public DistanceProfile(int p, int q, Map<Integer, Triplet<Double, Double, Double>> distances) {
        this.p = p;
        this.q = q;
        this.distances = Maps.newHashMap();
        distances.entrySet()
                .stream()
                .forEach(entry -> this.distances.put(entry.getKey(), 
                                new Triplet<>(entry.getValue().getValue0(),
                                        entry.getValue().getValue1(),
                                        entry.getValue().getValue2())));
    }
    
    /**
     * Populate the distance profile of p and q.
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param oracle distance oracle
     * @param maxS max s-distance to search
     * @param lb min component size
     * @param kind it indicates if p and q are (i) vertices, (ii) edges, (iii) one 
     * vertex and one edge
     * @param isSingle whether we want to find only the maxS-distance or all the s-distances up to maxS
     */
    public void createDistanceProfile(
            Map<Integer, Set<Integer>> vMap, 
            DistanceOracle oracle, 
            int maxS, 
            int lb, 
            String kind,
            boolean isSingle) {

        int start = 1;
        if (isSingle) {
            start = maxS;
        }
        for (int s = start; s <= maxS; s++) {
            switch (kind) {
                case "edge":
                    distances.put(s, oracle.getApproxSDistanceBetween(p, q, s, lb));
                    break;
                case "vertex":
                    distances.put(s, oracle.getApproxSDistanceBetweenVertices(vMap, p, q, s, lb));
                    break;
                default:
                    distances.put(s, oracle.getApproxSDistanceBetweenVE(vMap, p, q, s, lb));
                    break;
            }
        }
    }
    
    /**
     * 
     * @param s min overlap size
     * @param d (approximate) s-distance between p and q
     */
    public void addDistance(int s, double d) {
        distances.put(s, new Triplet<>(d, d, d));
    }
    
    /**
     * 
     * @param s min overlap size
     * @param ub upper-bound to the s-distance between p and q
     * @param lb lower-bound to the s-distance between p and q
     * @param d (approximate) s-distance between p and q
     */
    public void addDistance(int s, double lb, double ub, double d) {
        distances.put(s, new Triplet<>(lb, ub, d));
    }
    
    /**
     * 
     * @param s min overlap size
     * @param d upper-bound, lower-bound, and (approximate) s-distance between p and q
     */
    public void addDistance(int s, Triplet<Double, Double, Double> d) {
        distances.put(s, d);
    }
    
    /**
     * 
     * @return for each s, the approximate s-distance between p and q 
     */
    public Map<Integer, Triplet<Double, Double, Double>> getDistanceProfile() {
        return distances;
    }
    
    /**
     * 
     * @param s min overlap size
     * @return s-distance between p and q (if present)
     */
    public Double getSDistance(int s) {
        if (!distances.containsKey(s)) {
            return -1.;
        }
        return distances.get(s).getValue2();
    }
    
    /**
     * 
     * @param s min overlap size
     * @return lower-bound to s-distance between p and q (if present)
     */
    public Double getSLowBound(int s) {
        if (!distances.containsKey(s)) {
            return -1.;
        }
        return distances.get(s).getValue0();
    }
    
    /**
     * 
     * @param s min overlap size
     * @return upper-bound to s-distance between p and q (if present)
     */
    public Double getSUpBound(int s) {
        if (!distances.containsKey(s)) {
            return -1.;
        }
        return distances.get(s).getValue1();
    }
    
    public int getFirstId() {
        return p;
    }
    
    public int getSecondId() {
        return q;
    }
    
    public void printProfile() {
        System.out.print(p + "-" + q + "\n\t");
        System.out.print(distances.toString());
        System.out.println("\n-----");
    }
}
