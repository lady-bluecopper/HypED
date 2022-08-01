package eu.centai.hypeq.oracle.structures;

import com.google.common.collect.Lists;
import eu.centai.hypeq.oracle.ls.LandMarkSelector;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.centai.hyped.cc.ConnectedComponents;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Utils;
import eu.centai.hypeq.utils.Writer;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 *
 * @author giulia
 */
public class DistanceOracle {
    
    // from 2 edges to 3 edges
    double[] threeVPatterns = {1.3, 1.};
    // from 3 edges to 6 edges
    double[] fourVPatterns = {1.55, 1.3, 1.16, 1.};
    // from 4 edges to 10 edges
    double[] fiveVPatterns = {1.8, 1.58, 1.42, 1.3, 1.2, 1.1, 1.};
    
    // for each s, store s-distance oracle
    private SDistanceOracle[] oracles;
    // for each s, size of each s-connected component
    private Map<Integer, int[]> ccsSizes;
    // for each s, for each hyperedge e, store id of s-cc including e
    private Map<Integer, Map<Integer, Integer>> ccPerHyperedge;
    
    /**
     * Initialize the distance oracle.
     */
    public DistanceOracle() {
        this.ccsSizes = Maps.newHashMap();
        this.ccPerHyperedge = Maps.newHashMap();
    }
    
    /**
     * 
     * @return s-distance oracles of this oracle
     */
    public SDistanceOracle[] getSOracles() {
        return this.oracles;
    }
    
    /**
     * Method used when reading the oracle from disk.
     * 
     * @param oracles s-distance oracles to store in this oracle.
     */
    public void setSOracles(SDistanceOracle[] oracles) {
        this.oracles = oracles;
    }
    
    /**
     * 
     * @return sizes of the s-ccs stored in this oracle.
     */
    public Map<Integer, int[]> getCCsSizes() {
        return ccsSizes;
    }
    
    /**
     * Method used when reading the oracle from disk.
     * 
     * @param ccsSizes sizes of the s-ccs to store in this oracle.
     */
    public void setCCsSizes(Map<Integer, int[]> ccsSizes) {
        this.ccsSizes = ccsSizes;
    }
    
    /**
     * 
     * @return id of s-cc including each hyperedge, for each s.
     */
    public Map<Integer, Map<Integer, Integer>> getCCsMemberships() {
        return ccPerHyperedge;
    }
    
    /**
     * Method used when reading the oracle from disk.
     * 
     * @param ccPerHyperedge id of s-cc including each hyperedge, for each s.
     */
    public void setCCsMemberships(Map<Integer, Map<Integer, Integer>> ccPerHyperedge) {
        this.ccPerHyperedge = ccPerHyperedge;
    }
    
    /**
     * 
     * @param e hyperedge
     * @return the max s for which e belongs to some s-connected component; 1 otherwise
     */
    public int getMaxHEsMembership(int e) {
        return ccPerHyperedge.keySet()
                .stream()
                .filter(k -> ccPerHyperedge.get(k).containsKey(e))
                .mapToInt(x -> x)
                .max().orElse(1);
    }
    
    /**
     * Populate oracles simultaneously.
     * 
     * @param graph hypergraph for which the oracle is created
     * @param selMethod strategy to select the landmarks
     * @param assMethod strategy used to assign the number of landmarks to each s-cc
     * @param maxS max s for which a s-distance oracle should be created
     * @param numLandmarks integer used to compute max oracle size (numLandmarks * hypergraph size)
     * @param lb no landmark is assigned to s-connected components with size not greater than lb
     * @param importance if the strategy is ranking, it includes the importance 
     * factor for each ranking of the s-ccs; otherwise it includes the alpha and 
     * beta values for computing the probability of a s-cc to be selected.
     * In the latter case, alpha + beta <= 1.
     * @param isPrecomputed whether the hyperedge overlaps have been computed at creation time
     * @param seed seed for reproducibility
     * @throws java.io.FileNotFoundException
     * 
     */
    public void populateOracles(
            HyperGraph graph,
            String selMethod, 
            String assMethod,
            int maxS,
            int numLandmarks, 
            int lb,
            double[] importance,
            boolean isPrecomputed,
            int seed) throws FileNotFoundException {
        
        StopWatch watch = new StopWatch();
        watch.start();
        // find s-connected components for each s
        ConnectedComponents CCS = graph.findConnectedComponents(maxS);
        System.out.println("ccs found in (s) " + watch.getElapsedTimeInSec());
        // store useful info
        this.ccPerHyperedge = CCS.getCCPerHyperEdge();
        this.ccsSizes = CCS.getAllSCCs()
                .entrySet()
                .parallelStream()
                .map(entry -> {
                    int[] sizes = new int[entry.getValue().size()];
                    IntStream.range(0, sizes.length)
                            .forEach(i -> sizes[i] = entry.getValue().get(i).size());
                    return new Pair<Integer, int[]>(entry.getKey(), sizes);
                })
                .collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));
        // update neighbour data if needed
        watch.start();
        if (!isPrecomputed) {
            graph.initializeNeighboursFromCandidates(CCS, maxS, lb);
            System.out.println("neighbours found in (s) " + watch.getElapsedTimeInSec());
        }
        if (Settings.store) {
            Writer.writeConnectedComponents(CCS);
        }
        // clears temporary structures
        CCS.clearStructures();
        // initialize oracles
        this.oracles = new SDistanceOracle[CCS.size()];
        for (int s = 0; s < oracles.length; s++) {
            oracles[s] = new SDistanceOracle();
        }
        // compute max oracle size desired
        int budget = numLandmarks * graph.getNumEdges();
        // assign and find a number of landmarks to each s-cc, for each s
        System.out.println("searching landmarks with budget " + budget + "...");
        watch.start();
        LandMarkSelector ls = new LandMarkSelector(graph, selMethod, seed);
        Map<Integer, Set<Integer>> landsForAllS = ls.selectAllLandmarks(CCS, budget, lb, importance, assMethod);
        int numLands = landsForAllS.values().stream().mapToInt(l -> l.size()).sum();
        System.out.println(numLands + " landmarks found in (s) " + watch.getElapsedTimeInSec());
        // populate oracles
        watch.start();
        IntStream.range(0, oracles.length)
                .parallel()
                .forEach(s -> oracles[s].populateOracle(graph, landsForAllS.getOrDefault(s+1, Sets.newHashSet()), s+1));
        System.out.println("oracle populated in (s) " + watch.getElapsedTimeInSec());
    }
    
    /**
     * CCS-IS: creates the s-distance oracles separately, exploiting the
     * s-connected components.Landmarks are assigned using the sampling strategy.
     * Note that this method does not take the parameter isPrecomputed in input,
     * because it is always called together with {@link populateOracles}, which 
     * takes care of the initialization of the neighbors in the hypergraph.
     * 
     * @param graph hypergraph for which the oracle is created
     * @param selMethod strategy to select the landmarks
     * @param maxS max s for which a s-distance oracle should be created
     * @param numLandmarks integer used to compute max oracle size (numLandmarks * hypergraph size)
     * @param lb no landmark is assigned to s-connected components with size not greater than lb
     * @param importance importance factor for component size
     * @param seed seed for reproducibility
     * @return connected components found during the creation of the oracle
     */
    public ConnectedComponents populateOraclesCCIS(
            HyperGraph graph,
            String selMethod, 
            int maxS,
            int numLandmarks,
            int lb,
            double[] importance,
            int seed) {
        
        // initialize oracles
        this.oracles = new SDistanceOracle[maxS];
        // initialize connected components
        ConnectedComponents CCS = new ConnectedComponents();
        // compute max oracle size desired
        int budget = numLandmarks * graph.getNumEdges();
        // compute num landmarks per s, given a budget
        int budgetPerS = budget / maxS; 
        // populate one oracle at a time
        for (int s = 1; s <= maxS; s++) {
            oracles[s-1] = new SDistanceOracle();
            // find s-connected components
            List<Integer> edgeView = graph.getEdgesWithMinSize(s);
            List<List<Integer>> ccs = graph.findSConnectedComponents(edgeView, s);
            CCS.addMemberships(ccs, s);
            boolean notTrivial = CCS.addSCCs(ccs, s);
            // select landmarks
            if (!edgeView.isEmpty() && notTrivial) {
                LandMarkSelector ls = new LandMarkSelector(graph, selMethod, seed);
                Set<Integer> landmarks = ls.selectLandmarksPerCC(CCS.getSCCs(s), budgetPerS, lb, importance, s);
                oracles[s-1].populateOracle(graph, landmarks, s);
            }
        }
        // store useful info
        this.ccPerHyperedge = CCS.getCCPerHyperEdge();
        this.ccsSizes = CCS.getAllSCCs()
                .entrySet()
                .parallelStream()
                .map(entry -> {
                    int[] sizes = new int[entry.getValue().size()];
                    IntStream.range(0, sizes.length)
                            .forEach(i -> sizes[i] = entry.getValue().get(i).size());
                    return new Pair<Integer, int[]>(entry.getKey(), sizes);
                })
                .collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));
        return CCS;
    }
    
    /**
     * Baseline: creates the s-distance oracles separately, with no knowledge of 
     * the s-connected components.
     * 
     * @param graph hypergraph for which the oracle is created
     * @param selMethod strategy to select the landmarks
     * @param maxS max s for which a s-distance oracle should be created
     * @param numLandmarks integer used to compute max oracle size (numLandmarks * hypergraph size)
     * @param seed seed for reproducibility
     */
    public void populateOracleBaseline(
            HyperGraph graph,
            String selMethod, 
            int maxS,
            int numLandmarks,
            int seed) {
        
        // initialize oracles
        this.oracles = new SDistanceOracle[maxS];
        for (int s = 0; s < maxS; s++) {
            oracles[s] = new SDistanceOracle();
        }
        // set budget for oracle
        int budget = numLandmarks * graph.getNumEdges();
        int sizePerS = maxS * graph.getNumEdges();
        // initialize selector
        LandMarkSelector ls = new LandMarkSelector(graph, selMethod, seed);
        ls.initializeCache();
        // populate one oracle at a time, until we reach the desired budget
        int[] sizes = new int[maxS];
        int estOracleSize = 0;
        int numLandsForS;
        while (estOracleSize < budget) {
            numLandsForS = Math.max(1, (budget - estOracleSize) / sizePerS);
            for (int s = 1; s <= maxS; s++) {
                Set<Integer> landmarks = ls.selectLandmarks(oracles[s-1].getLandmarks(), numLandsForS, s);
                oracles[s-1].populateOracle(graph, landmarks, s); 
                sizes[s-1] = oracles[s-1].getOracleSize();
                estOracleSize = Arrays.stream(sizes).sum();
                if (estOracleSize >= budget) {
                    break;
                }
            }
        }
    }
    
    /**
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param v element id (vertex or hyperedge)
     * @param label label of the element (if any); NONE otherwise
     * @param kind type of s-distance to compute (vertex, edge, both)
     * @return the s-distances, for all the elements reachable from v
     */
    public ReachableProfile createReachableProfile(
            Map<Integer, Set<Integer>> vMap, 
            int v, 
            String label,
            String kind) {
        ReachableProfile sp = new ReachableProfile(v, label);
        for (int s = 1; s <= oracles.length; s++) {
            Map<Integer, Integer> lbs = Maps.newHashMap();
            Map<Integer, Integer> ubs = Maps.newHashMap();
            Map<Integer, Map<Integer, Integer>> distances = oracles[s-1].getLabels();
            // if kind = edge, it includes only v; otherwise it includes all the 
            // hyperedges including v
            List<Integer> sources = Lists.newArrayList();
            if (Settings.kind.equalsIgnoreCase("edge")) {
                sources.add(v);
            } else {
                sources.addAll(vMap.getOrDefault(v, Sets.newHashSet()));
            }
            for (int e1 : sources) {
                // find lower and upper bounds to the s-distances to all the 
                // reachable hyperedges
                Map<Integer, Integer> map1 = distances.getOrDefault(e1, Maps.newHashMap());
                for (int e2 : distances.keySet()) {
                    if (e1 == e2) {
                        // save distances from landmarks
                        map1.entrySet().forEach(entry -> {
                            lbs.put(entry.getKey(), Math.max(lbs.getOrDefault(entry.getKey(), -1), entry.getValue()));
                            ubs.put(entry.getKey(), Math.min(ubs.getOrDefault(entry.getKey(), Integer.MAX_VALUE), entry.getValue()));
                        });
                    } else {
                        // use landmarks to get distances to all the other reachable hyperedges
                        Map<Integer, Integer> map2 = distances.get(e2);
                        map2.entrySet().forEach(entry -> {
                            if (map1.containsKey(entry.getKey())) {
                                int thisUpper = map1.get(entry.getKey()) + entry.getValue();
                                int thisLower = Math.abs(map1.get(entry.getKey()) - entry.getValue());
                                lbs.put(e2, Math.max(lbs.getOrDefault(e2, -1), thisLower));
                                ubs.put(e2, Math.min(ubs.getOrDefault(e2, Integer.MAX_VALUE), thisUpper));
                            }
                        });
                    }
                }
            }
            
            if (!kind.equalsIgnoreCase("vertex")) {
                for (Entry<Integer, Integer> entry : lbs.entrySet()) {
                    double est = entry.getValue() + (ubs.get(entry.getKey()) - entry.getValue()) / 2;
                    sp.addReachable(entry.getKey(), s, est);
                }
            // if kind is vertex, we need to find the distances to the vertices
            } else {
                // find lower and upper bounds to the s-distances from v to all the
                // reachable vertices
                Map<Integer, Integer> vertexlbs = Maps.newHashMap();
                Map<Integer, Integer> vertexubs = Maps.newHashMap();
                Map<Integer, List<Integer>> invVMap = Maps.newHashMap();
                for (int w : vMap.keySet()) {
                    vMap.get(w).stream()
                            .filter(he -> lbs.containsKey(he))
                            .forEach(he -> {
                                List<Integer> tmp = invVMap.getOrDefault(he, Lists.newArrayList());
                                tmp.add(w);
                                invVMap.put(he, tmp);
                            });
                }
                for (int eid : lbs.keySet()) {
                    for (int u : invVMap.get(eid)) {
                    vertexlbs.put(u, Math.max(vertexlbs.getOrDefault(u, -1), lbs.get(eid)));
                    vertexubs.put(u, Math.min(vertexubs.getOrDefault(u, Integer.MAX_VALUE), ubs.get(eid)));
                    }
                }
                for (int u : vertexlbs.keySet()) {
                    double est = vertexlbs.get(u) + (vertexubs.get(u) - vertexlbs.get(u)) / 2;
                    sp.addReachable(u, s, est);
                }
            }
        }
        System.out.println("Reachable Profile for " + v + " has Size = " + sp.getReachables().size());
        return sp;
    }
    
    /**
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param v vertex id
     * @param s min overlap size
     * @return list of hyperedges of size not lower than s and including v
     */
    public List<Integer> getSHyperEdgesOf(Map<Integer, Set<Integer>> vMap, int v, int s) {
        return vMap.getOrDefault(v, Sets.newHashSet())
                .stream()
                .filter(e -> ccPerHyperedge.getOrDefault(s, Maps.newHashMap()).containsKey(e))
                .collect(Collectors.toList());
    }
    
    /**
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param u vertex id
     * @param v vertex id
     * @return true if they belong to a common hyperedge; false otherwise
     */
    public boolean inCommonHyperEdge(Map<Integer, Set<Integer>> vMap, int u, int v) {
        return Utils.intersect(vMap.getOrDefault(u, Sets.newHashSet()), vMap.getOrDefault(v, Sets.newHashSet()));
    }
    
    /**
     * If the s-cc including e1 and e2 has size not greater than lb, the distance
     * between e1 and e2 is approximated according to the topology of the s-cc.
     * 
     * @param e1 hyperedge id
     * @param e2 hyperedge id
     * @param s min overlap size
     * @param lb min component size
     * @return lower-bound, upper-bound, and approximate s-distance between e1 and e2
     */
    public Triplet<Double, Double, Double> getApproxSDistanceBetween(int e1, int e2, int s, int lb) {
        if (!ccPerHyperedge.isEmpty()) {
            if (!ccPerHyperedge.containsKey(s)) {
                return new Triplet<>(-1., -1., -1.);
            }
            int cc1 = getIdOfSCC(e1, s);
            int cc2 = getIdOfSCC(e2, s);
            // if they do not belong to the same s-connected component
            // or at least one of them does not belong to any s-cc
            if (cc1 != cc2 || cc1 == -1 || cc2 == -1) {
                return new Triplet<>(-1., -1., -1.);
            }
            // if they belong to a small s-cc
            int size = getSizeOf(s, cc1);
            if (size <= lb && !oracles[s-1].hasLabel(e1)) {
                return new Triplet<>(1., 1. * size, getApproxDistance(size, 1));
            }
        }
        // in all the other cases
        int pUp = Integer.MAX_VALUE;
        double pLo = -1;
        Map<Integer, Integer> otMap1 = oracles[s-1].getLabel(e1);
        Map<Integer, Integer> otMap2 = oracles[s-1].getLabel(e2);
        // find lower and upper bounds                    
        for (Map.Entry<Integer, Integer> entry : otMap2.entrySet()) {
            if (otMap1.containsKey(entry.getKey())) {
                int thisUpper = otMap1.get(entry.getKey()) + entry.getValue();
                int thisLower = Math.abs(otMap1.get(entry.getKey()) - entry.getValue());
                pUp = Math.min(pUp, thisUpper);
                pLo = Math.max(pLo, thisLower);
            }
        }
        if ((pUp == -1) || (pLo == -1)) {
            return new Triplet<>(-1., -1., -1.);
        }
        // return lower-bound, upper-bound, median
        return new Triplet<>(pLo, 1.*pUp, pLo + (pUp - pLo) / 2);
    }
    
    /**
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param v1 vertex id
     * @param v2 vertex id
     * @param s min overlap size
     * @param lb lower-bound to component size
     * @return lower-bound, upper-bound, and approximate s-distance between v1 and v2
     */
    public Triplet<Double, Double, Double> getApproxSDistanceBetweenVertices(
            Map<Integer, Set<Integer>> vMap, 
            int v1, 
            int v2, 
            int s, 
            int lb) {
        if (inCommonHyperEdge(vMap, v1, v2)) {
            return new Triplet<>(1., 1., 1.);
        }
        Triplet<Double, Double, Double> approxVDist = new Triplet<>(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
        for (int e1 : getSHyperEdgesOf(vMap, v1, s)) {
            for (int e2 : getSHyperEdgesOf(vMap, v2, s)) {
                Triplet<Double, Double, Double> approxDist = getApproxSDistanceBetween(e1, e2, s, lb);
                if (approxDist.getValue0() != -1 && approxDist.getValue2() < approxVDist.getValue2()) {
                    approxVDist = approxDist;
                }
            }
        }
        if (approxVDist.getValue2() != Double.MAX_VALUE) {
            return approxVDist;
        }
        return new Triplet<>(-1., -1., -1.);
    }
    
    /**
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param v vertex id
     * @param e hyperedge id
     * @param s min overlap size
     * @param lb min component size
     * @return lower-bound, upper-bound, and approximate s-distance between v and e
     */
    public Triplet<Double, Double, Double> getApproxSDistanceBetweenVE(
            Map<Integer, Set<Integer>> vMap, 
            int v, 
            int e, 
            int s, 
            int lb) {
        Triplet<Double, Double, Double> approxVDist = new Triplet<>(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
        for (int e1 : getSHyperEdgesOf(vMap, v, s)) {
            Triplet<Double, Double, Double> approxDist = getApproxSDistanceBetween(e1, e, s, lb);
            if (approxDist.getValue0() != -1 && approxDist.getValue2() < approxVDist.getValue2()) {
                approxVDist = approxDist;
            }
        }
        if (approxVDist.getValue2() != Double.MAX_VALUE) {
            return approxVDist;
        }
        return new Triplet<>(-1., -1., -1.);
    }
    
    /**
     * Approximate s-distance based on topology of the s-connected component.
     * 
     * @param size size of the s-connected component
     * @param lb number of known s-overlaps between hyperedges in the s-connected component
     * @return approximate average s-distance among hyperedges in the s-connected component
     */
    private double getApproxDistance(int size, int lb) {
        // the pattern is a vertex
        if (size == 1) {
            return 0;
        }
        // the pattern is an edge
        if (size == 2) {
            return 1;
        }
        int i = Math.max(0, lb - size + 1);
        double sum = 0;
        double div = 0;
        switch (size) {
            case 3:
                for (int j = i; j < threeVPatterns.length; j++) {
                    sum += threeVPatterns[j];
                    div += 1;
                }   break;
            case 4:
                for (int j = i; j < fourVPatterns.length; j++) {
                    sum += fourVPatterns[j];
                    div += 1;
                }   break;
            case 5:
                for (int j = i; j < fiveVPatterns.length; j++) {
                    sum += fiveVPatterns[j];
                    div += 1;
                }   break;
        }
        return sum/div;
    }
    
    /**
     * 
     * @return size of this oracle computed as number of s-distance pairs stored
     */
    public int getOracleSize() {
        return IntStream.range(0, oracles.length).map(i -> oracles[i].getOracleSize()).sum();
    }
    
    /**
     * 
     * @return number of integers in the structures storing the info about the ccs
     */
    public int getCCStrucSize() {
        int sizes = ccsSizes.values().stream().mapToInt(l -> l.length).sum();
        int membs = ccPerHyperedge.values().stream().mapToInt(d -> d.size()).sum();
        return sizes + membs;
    }
    
    /**
     * 
     * @return number of s-distance oracles used by this oracle
     */
    public int getNumSOracles() {
        return oracles.length;
    }
    
    /**
     * 
     * @return number of landmarks used by this oracle
     */
    public int getNumLandmarks() {
        return IntStream.range(0, oracles.length).map(i -> oracles[i].getNumLandmarks()).sum();
    }
    
    /**
     * 
     * @return the set of landmarks selected for each s
     */
    public List<Set<Integer>> getAllLandmarks() {
        List<Set<Integer>> allLands = Lists.newArrayList();
        for (SDistanceOracle oracle : oracles) {
            allLands.add(oracle.getLandmarks());
        }
        return allLands;
    }
    
    /**
     * 
     * @param s min overlap size
     * @return s-distance oracle
     */
    public SDistanceOracle getOracle(int s) {
        return oracles[s-1];
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
     * @param s min overlap size
     * @param cc_id id of s-cc
     * @return size of the s-cc with id cc_id
     */
    public int getSizeOf(int s, int cc_id) {
        int[] tmp = ccsSizes.getOrDefault(s, new int[0]);
        if (tmp.length <= cc_id) {
            return 0;
        }
        return tmp[cc_id];
    }
    
    /**
     * Sample pairs of hyperedges/vertex-hyperedge ensuring that they belong to 
     * the same s-connected component for some s.
     * 
     * @param vMap for each vertex, the hyperedges to which it belongs
     * @param sampleSize number of pairs of hyperedges to sample
     * @param seed seed for reproducibility
     * @param kind if "edge" find edge pairs; if "both" find vertex-edge pairs
     * @return a sample of pairs 
     */
    public Set<Pair<Integer, Integer>> samplePairs(
            Map<Integer, Set<Integer>> vMap,
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
            // initialize inverse map hyperedge -> vertices contained
            Map<Integer, Set<Integer>> invMap = Maps.newHashMap();
            for (int v : vMap.keySet()) {
                for (int e : vMap.get(v)) {
                    Set<Integer> tmp = invMap.getOrDefault(e, Sets.newHashSet());
                    tmp.add(v);
                    invMap.put(e, tmp);
                }
            }
            for (Pair<Integer, Integer> p : hyperedgeSample) {
                Pair<Integer, Integer> pair;
                List<Integer> cands = Lists.newArrayList(invMap.getOrDefault(p.getValue0(), Sets.newHashSet()));
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
        int sumS = ccPerHyperedge.keySet().stream().mapToInt(i -> i).sum();
        int maxS = ccPerHyperedge.keySet().stream().mapToInt(i -> i).max().orElse(0);
        // temporary map
        Map<Integer, List<Integer>> invMap;
        for (int s = maxS; s > 1; s--) {
            // create temporary inverse map
            invMap = Maps.newHashMap();
            for (Entry<Integer, Integer> en : ccPerHyperedge.get(s).entrySet()) {
                List<Integer> lst = invMap.getOrDefault(en.getValue(), Lists.newArrayList());
                lst.add(en.getKey());
                invMap.put(en.getValue(), lst);
            }
            // try to add b pairs to the sample
            int b = s * (sampleSize - sample.size()) / sumS;
            Utils.sampleInLists(Lists.newArrayList(invMap.values()), sample, sample.size() + b, rnd);
            sumS -= s;
        }
        invMap = Maps.newHashMap();
        for (Entry<Integer, Integer> en : ccPerHyperedge.get(1).entrySet()) {
            List<Integer> lst = invMap.getOrDefault(en.getValue(), Lists.newArrayList());
            lst.add(en.getKey());
            invMap.put(en.getValue(), lst);
        }
        Utils.sampleInLists(Lists.newArrayList(invMap.values()), sample, sampleSize, rnd);
        return sample;
    }
    
}
