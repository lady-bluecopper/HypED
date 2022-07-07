package eu.centai.hyped.cc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.Utils;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.javatuples.Pair;

/**
 *
 * @author giulia
 */
public class ConnectedComponents {
    
    private Map<Integer, List<List<Integer>>> ccs; // s-connected components for each s
    // partial overlaps between hyperedges
    private Map<Pair<Integer, Integer>, Integer> partialOverlaps; 
    // overlapping hyperedges that belong to the same s-connected component,
    // and could be direct neighbours
    private Map<Integer, Map<Integer, List<Integer>>> candsNeigh; 
    // for each s, for each hyperedge e, store id of s-cc including e
    private Map<Integer, Map<Integer, Integer>> ccPerHyperedge;
    
    public ConnectedComponents() {
        this.ccs = Maps.newHashMap();
        this.partialOverlaps = Maps.newHashMap();
        this.candsNeigh = Maps.newHashMap();
        this.ccPerHyperedge = Maps.newHashMap();
    }
    
    /**
     * Empties the temporary maps.
     */
    public void clearStructures() {
        this.partialOverlaps.clear();
        this.candsNeigh.clear();
    }
    
    /**
     * 
     * @param e1 hyperedge id
     * @param e2 hyperedge id
     * @return partial overlap between e1 and e2 if already computed; -1 otherwise
     */
    public int getOverlap(int e1, int e2) {
        return partialOverlaps.getOrDefault(new Pair<>(e1, e2), -1);
    }
    
    /**
     * 
     * @return all the partial overlaps computed so far
     */
    public Map<Pair<Integer, Integer>, Integer> getOverlaps() {
        return partialOverlaps;
    }
    
    /**
     * Method used when reading the connected components from disk.
     * 
     * @param partialOverlaps all the partial overlaps
     */
    public void setOverlaps(Map<Pair<Integer, Integer>, Integer> partialOverlaps) {
        this.partialOverlaps = partialOverlaps;
    }
    
    /**
     * 
     * @param e1 hyperedge id
     * @param e2 hyperedge id
     * @param o partial overlap between e1 and e2
     */
    public void addOverlap(int e1, int e2, int o) {
        partialOverlaps.put(new Pair<>(e1, e2), o);
    }
    
    /**
     * Adds a pair of hyperedges with overlap that could be >= s.
     * 
     * @param e1 hyperedge id
     * @param e2 hyperedge id
     * @param s s for which e1 and e2 belong to the same s-connected component
     */
    public void addCandidateNeighbourPair(int e1, int e2, int s) {
        Map<Integer, List<Integer>> tmp = candsNeigh.getOrDefault(s, Maps.newHashMap());
        List<Integer> tmpList = tmp.getOrDefault(e1, Lists.newArrayList());
        tmpList.add(e2);
        tmp.put(e1, tmpList);
        candsNeigh.put(s, tmp);
    }
    
    /**
     * 
     * @return candidate neighbors for each hyperedge, discovered during the 
     * search for the connected components
     */
    public Collection<Map<Integer, List<Integer>>> getCandsNeigh() {
        return candsNeigh.values();
    }
    
    /**
     * Method used when reading the oracle from disk.
     * 
     * @param cands candidate neighbors for each hyperedge
     */
    public void setCandsNeigh(Collection<Map<Integer, List<Integer>>> cands) {
        for (Map<Integer, List<Integer>> map : cands) {
            this.candsNeigh.put(candsNeigh.size(), map);
        }
    }
    
    /**
     * 
     * @return pairs of overlapping hyperedges found so far
     */
    public Collection<Pair<Integer, Integer>> getOverlappingPairs() {
        return partialOverlaps.keySet();
    }
    
    /**
     * Stores the s-connected components of the hypergraph, if they are not all
     * singletons.
     * 
     * @param thisCCs s-connected components
     * @param s min overlap size
     * @return true if the s-connected components have been stored; false otherwise
     */
    public boolean addSCCs(List<List<Integer>> thisCCs, int s) {
        int maxSize = thisCCs.stream().mapToInt(l -> l.size()).max().orElse(0);
        if (maxSize > 1) {
            ccs.put(s, thisCCs);
            return true;
        }
        return false;
    }
    
    /**
     * Stores, for each hyperedge e, the id of the s-cc including e
     * @param thisCCs s-connected components
     * @param s min overlap size
     */
    public void addMemberships(List<List<Integer>> thisCCs, int s) {
        Map<Integer, Integer> tmp = Maps.newHashMap();
        for (int i = 0; i < thisCCs.size(); i++) {
            final int id = i;
            tmp.putAll(thisCCs.get(i).parallelStream()
                    .map(e -> new Pair<Integer, Integer>(e, id))
                    .collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1())));
            
        }
        ccPerHyperedge.put(s, tmp);
    }
    
    /**
     * 
     * @param e hyperedge
     * @param s min overlap size
     * @return id of the s-cc including e, if any; -1 otherwise
     */
    public int getIdOfSCC(int e, int s) {
        return ccPerHyperedge.getOrDefault(s, Maps.newHashMap()).getOrDefault(e, -1);
    }
    
    /**
     * 
     * @param ccPerHyperedge for each s, for each hyperedge e, id of the s-cc including e
     */
    public void setCCPerHyperedge(Map<Integer, Map<Integer, Integer>> ccPerHyperedge) {
        this.ccPerHyperedge = ccPerHyperedge;
    }
    
    /**
     * 
     * @param s min overlap size
     * @return s-connected components
     */
    public List<List<Integer>> getSCCs(int s) {
        return ccs.getOrDefault(s, Lists.newArrayList());
    }
    
    /**
     * Stores the s-connected components for each s.
     * 
     * @param ccs s-connected components for each s
     */
    public void setAllSCCs(Map<Integer, List<List<Integer>>> ccs) {
        this.ccs = ccs;
    }
    
    /**
     * 
     * @param s min overlap size
     * @param cc_id id of s-cc
     * @return size of the s-cc with id cc_id
     */
    public int getSizeOf(int s, int cc_id) {
        List<List<Integer>> tmp = ccs.getOrDefault(s, Lists.newArrayList());
        if (tmp.size() <= cc_id) {
            return 0;
        }
        return tmp.get(cc_id).size();
    }
    
    /**
     * 
     * @param s min overlap size
     * @return true if there exists some s-cc
     */
    public boolean contains(int s) {
        return ccs.containsKey(s);
    }
    
    /**
     * 
     * @return true if the connected components have not been found yet
     */
    public boolean isEmpty() {
        return ccs.isEmpty();
    }
    
    /**
     * 
     * @return for each s, the s-connected components stored
     */
    public Map<Integer, List<List<Integer>>> getAllSCCs() {
        return ccs;
    }
    
    /**
     * 
     * @return number of s values, for which the s-connected components are stored
     */
    public int size() {
        return ccs.size();
    }
    
    /**
     * 
     * @return for each s, for each hyperedge e, the id of the s-cc including e 
     */
    public Map<Integer, Map<Integer, Integer>> getCCPerHyperEdge() {
        return ccPerHyperedge;
    }
    
}
