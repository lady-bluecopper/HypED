package eu.centai.hypeq.oracle.structures;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.centai.hypeq.structures.HyperGraph;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author giulia
 */
public class SDistanceOracle {
    
    private Set<Integer> landmarks;
    private Map<Integer, Map<Integer, Integer>> labels; // s-distances from hyperedges to landmarks
    private int numLandmarks;
    
    public SDistanceOracle() {
        this.landmarks = Sets.newHashSet();
        this.labels = Maps.newHashMap();
    }
    
    /**
     * Find all the s-distances from a set of landmarks to the reachable hyperedges.
     * 
     * @param graph hypergraph
     * @param lands set of landmarks
     * @param s min overlap size
     */
    public void findDistances(HyperGraph graph, Set<Integer> lands, int s) {
        for (int l : lands) {
            Map<Integer, Integer> dist = graph.findDistancesFrom(l, s);
            dist.entrySet().stream().forEach(e -> {
                Map<Integer, Integer> tmpDist = labels.getOrDefault(e.getKey(), Maps.newHashMap());
                tmpDist.put(l, e.getValue());
                labels.put(e.getKey(), tmpDist);
            });
        }
    }
    
    /**
     * Find distances from the given set of landmarks to all the other hyperedges.
     * Method used when all the oracles are created simultaneously.
     *
     * @param graph hypergraph
     * @param landmarks set of landmarks
     * @param s min overlap size
     */
    public void populateOracle(HyperGraph graph, Set<Integer> landmarks, int s) {
        this.landmarks.addAll(landmarks);
        this.numLandmarks = landmarks.size();
        findDistances(graph, landmarks, s);
    }
    
    /**
     * 
     * @return set of landmarks used by the oracle
     */
    public Set<Integer> getLandmarks() {
        return landmarks;
    }
    
    /**
     * 
     * @return number of landmarks used by the oracle
     */
    public int getNumLandmarks() {
        return numLandmarks;
    }
    
    /**
     * Method used when reading the oracle from disk;
     * @param numLandmarks number of landmarks used by the oracle
     */
    public void setNumLandmarks(int numLandmarks) {
        this.numLandmarks = numLandmarks;
    }

    /**
     * 
     * @return s-distances from landmarks to hyperedges
     */
    public Map<Integer, Map<Integer, Integer>> getLabels() {
        return labels;
    }
    
    /**
     * 
     * @param labels s-distances from landmarks to hyperedges
     */
    public void setLabels(Map<Integer, Map<Integer, Integer>> labels) {
        this.labels = labels;
    }
    
    /**
     *
     * @param e hyperEdge
     * @return s-distances from e to hyperedges reachable from e 
     */
    public Map<Integer, Integer> getLabel(int e) {
        return labels.getOrDefault(e, Maps.newHashMap());
    }
    
    /**
     * 
     * @param e hyperedge
     * @return true if the oracle stores s-distances from e; false otherwise
     */
    public boolean hasLabel(int e) {
        return labels.containsKey(e);
    }
    
    /**
     * 
     * @return number of distance pairs stored in this oracle.
     */
    public int getOracleSize() {
        return labels.values().stream().mapToInt(m -> m.size()).sum();
    }
    
}
