package eu.centai.hyped.cc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.utils.Settings;
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
    
    /**
     * Sample pairs of hyperedges ensuring that they belong to the same 
     * s-connected component for some s.
     * 
     * @param sampleSize number of pairs of hyperedges to sample
     * @param seed seed for reproducibility
     * @return a sample of pairs of hyperedges 
     */
    public Set<Pair<Integer, Integer>> sampleHyperEdges(int sampleSize, int seed) {
        Set<Pair<Integer, Integer>> sample = Sets.newHashSet();
        Random rnd = new Random(seed);
        int sumS = ccs.keySet().stream().mapToInt(i -> i).sum();
        int maxS = size();
        for (int s = maxS; s > 1; s--) {
            // try to add b pairs to the sample
            int b = s * (sampleSize - sample.size()) / sumS;
            sampleInCCs(ccs.get(s), sample, sample.size() + b, rnd);
            sumS -= s;
        }
        sampleInCCs(ccs.get(1), sample, sampleSize, rnd);
        return sample;
    }
    
    /**
     * Sample pairs of hyperedges/vertex-hyperedge ensuring that they belong to 
     * the same s-connected component for some s.
     * 
     * @param graph hypergraph
     * @param sampleSize number of pairs of hyperedges to sample
     * @param seed seed for reproducibility
     * @param kind if "edge" find edge pairs; if "both" find vertex-edge pairs
     * @return a sample of pairs 
     */
    public Set<Pair<Integer, Integer>> samplePairs(
            HyperGraph graph,
            int sampleSize, 
            int seed, 
            String kind) {
        Set<Pair<Integer, Integer>> hyperedgeSample = sampleHyperEdges(sampleSize, seed);
        if (kind.equalsIgnoreCase("edge")) {
            return hyperedgeSample;
        } else if (kind.equalsIgnoreCase("both")) {
            Random rand = new Random(Settings.seed);
            Set<Pair<Integer, Integer>> sample = Sets.newHashSet();
            Set<Integer> verticesAdded = Sets.newHashSet();
            for (Pair<Integer, Integer> p : hyperedgeSample) {
                Pair<Integer, Integer> pair;
                List<Integer> cands = Lists.newArrayList(graph.getVerticesOf(p.getValue0()));
                if (verticesAdded.containsAll(cands)) {
                    continue;
                }
                do {
                    pair = new Pair<>(cands.get(rand.nextInt(cands.size())), p.getValue1());
                } while (!sample.add(pair));
                verticesAdded.add(pair.getValue0());
            }
            return sample;
        } else {
            throw new IllegalArgumentException("Kind " + kind + " not supported.");
        }
    }
    
    /**
     * Tries to add *budget* pairs of hyperedges to the sample.
     * Some pairs may already been present in the sample, due to previous
     * iterations.
     * 
     * @param sccs list of s-connected components
     * @param sample pairs sampled in previous iterations
     * @param budget number of pairs to sample
     * @param rnd random object
     * @return the pairs of hyperedges sampled from sccs
     */
    private void sampleInCCs(
            List<List<Integer>> sccs, 
            Set<Pair<Integer, Integer>> sample,
            int budget, 
            Random rnd) {
        
        List<Integer> selectable = IntStream.range(0, sccs.size())
                .filter(i -> sccs.get(i).size() > 2)
                .boxed()
                .collect(Collectors.toList());
        
        BigInteger numCandPairs = BigInteger.ZERO;
        BigInteger one = BigInteger.ONE;
        BigInteger two = one.add(one);
        BigInteger B = BigInteger.valueOf(budget - sample.size());
        for (int i : selectable) {
            BigInteger size = BigInteger.valueOf(sccs.get(i).size());
            if (size.compareTo(BigInteger.ZERO) > 0) {
                // handle int overflow
                if (size.compareTo(B) > 0) {
                    numCandPairs = size;
                    break;
                }
                BigInteger prod = size.multiply(size.subtract(one));
                numCandPairs = numCandPairs.add(prod.divide(two));
                if (numCandPairs.compareTo(B) > 0) {
                    break;
                }
            }
        }
        // if we do not have enough pairs, add all of them
        if (numCandPairs.compareTo(B) <= 0) {
            selectable.stream().forEach(i -> {
                Iterator<int[]> it = CombinatoricsUtils.combinationsIterator(sccs.get(i).size(), 2);
                while (it.hasNext()) {
                    int[] comb = it.next();
                    int fst = sccs.get(i).get(comb[0]);
                    int sec = sccs.get(i).get(comb[1]);
                    sample.add(fst < sec
                            ? new Pair<>(fst, sec) 
                            : new Pair<>(sec, fst));
                }
            });
            return;
        }
        // otherwise we select them at random
        while (sample.size() < budget) {
            List<Integer> selectedCC = sccs.get(selectable.get(rnd.nextInt(selectable.size())));
            int first = selectedCC.get(rnd.nextInt(selectedCC.size()));
            int second;
            do {
                second = selectedCC.get(rnd.nextInt(selectedCC.size()));
            } while (first==second);
            sample.add(first < second 
                            ? new Pair<>(first, second) 
                            : new Pair<>(second, first));
        }
    }
    
 }
