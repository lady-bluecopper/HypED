package eu.centai.hypeq.test.helpers;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.centai.hyped.cc.ConnectedComponents;
import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Writer;
import gr.james.sampling.LiLSampling;
import gr.james.sampling.RandomSamplingCollector;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 * Helper methods for the various tests.
 * 
 * @author giulia
 */
public class Helper {
    
    /**
     * Populates the approximate distance profiles for the pairs of elements in 
     * realDist, using a distance oracle.
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param oracle distance oracle
     * @param sample sample
     * @return approximate distance profiles computing using the distance oracle
     * @throws IOException 
     */
    public static Map<Pair<Integer, Integer>, DistanceProfile> populateDistanceProfiles(
            Map<Integer, Set<Integer>> vMap,
            DistanceOracle oracle,
            Collection<Pair<Integer, Integer>> sample) throws IOException {
        
        Map<Pair<Integer, Integer>, DistanceProfile> allApproxHDist = sample.parallelStream()
                .map(entry -> {
                    int u = entry.getValue0();
                    int v = entry.getValue1();
                    int maxS = Settings.maxS;
                    if (Settings.kind.equalsIgnoreCase("edge")) {
                        maxS = Math.min(Math.min(oracle.getMaxHEsMembership(u), oracle.getMaxHEsMembership(v)), Settings.maxS);
                    } else if (Settings.kind.equalsIgnoreCase("vertex")) {
                        int s1 = oracle.getSHyperEdgesOf(vMap, u, 1)
                                .stream()
                                .mapToInt(e -> oracle.getMaxHEsMembership(e)).max().orElse(0);
                        int s2 = oracle.getSHyperEdgesOf(vMap, v, 1)
                                .stream()
                                .mapToInt(e -> oracle.getMaxHEsMembership(e)).max().orElse(0);
                        maxS = Math.min(s1, s2);
                    } else if (Settings.kind.equalsIgnoreCase("both")) {
                        int s1 = oracle.getSHyperEdgesOf(vMap, u, 1)
                                .stream()
                                .mapToInt(e -> oracle.getMaxHEsMembership(e)).max().orElse(0);
                        int s2 = oracle.getMaxHEsMembership(v);
                        maxS = Math.min(s1, s2);
                    }
                    DistanceProfile dp = new DistanceProfile(u, v);
                    dp.createDistanceProfile(vMap, oracle, maxS, Settings.lb, Settings.kind, false);
                    return new Pair<Pair<Integer, Integer>, DistanceProfile>(entry, dp);
                })
                .collect(Collectors.toMap(e -> e.getValue0(), e -> e.getValue1()));
        return allApproxHDist;
    }
    
    /**
     * Populates the approximate distance profiles for the pairs of elements in 
     * realDist, using a distance oracle.
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param oracle distance oracle
     * @param queries triplets (src,dest,s) of s-distances to estimate
     * @return approximate distance profiles computing using the distance oracle
     * @throws IOException 
     */
    public static Map<Pair<Integer, Integer>, DistanceProfile> answerSDistanceQueries(
            Map<Integer, Set<Integer>> vMap,
            DistanceOracle oracle,
            Collection<Triplet<Integer, Integer, Integer>> queries) throws IOException {
        
        Map<Pair<Integer, Integer>, DistanceProfile> allApproxHDist = queries.parallelStream()
                .map(entry -> {
                    int u = entry.getValue0();
                    int v = entry.getValue1();
                    DistanceProfile dp = new DistanceProfile(u, v);
                    dp.createDistanceProfile(vMap, oracle, entry.getValue2(), Settings.lb, Settings.kind, true);
                    return new Pair<Pair<Integer, Integer>, DistanceProfile>(new Pair<>(u, v), dp);
                })
                .collect(Collectors.toMap(e -> e.getValue0(), e -> e.getValue1()));
        return allApproxHDist;
    }
    
    /**
     * Finds the approximate s-centrality for the query elements, 
     * using a distance oracle.
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param oracle distance oracle
     * @param queries elements for which the s-centrality value must be computed
     * @param s min overlap size
     * @param kind kind of distance to compute, among "edge" (edge to edge),
     * "vertex" (vertex to vertex), and "both" (vertex to edge)
     * @return approximate s-centrality for each query element, computed 
     * using the distance oracle
     * @throws IOException 
     */
    public static Map<Integer, Double> findASCentralities(
            Map<Integer, Set<Integer>> vMap,
            DistanceOracle oracle,
            Collection<Integer> queries,
            int s,
            String kind) throws IOException {
        
        return queries.parallelStream()
                .map(e -> {
                    if (kind.equalsIgnoreCase("edge")) {
                        // check if hyperedge has size < s
                        int cc_id = oracle.getIdOfSCC(e, s);
                        if (cc_id == -1) {
                            return new Pair<>(e, 0.);
                        }
                        Map<Integer, Integer> distances = oracle.getOracle(s).getLabel(e);
                        // check if hyperedge belongs to a small s-cc
                        if (distances.isEmpty()) {
                            int cc_size = oracle.getSizeOf(s, cc_id);
                            double approxSum = oracle.getApproxDistance(cc_size, 1) * (cc_size - 1);
                            return new Pair<>(e, (cc_size - 1) / approxSum);
                        }
                        // find sum of distances to reachable hyperedges
                        double sum = distances.values().stream().mapToInt(i -> i).sum();
                        int numElem = distances.size() - 1;
                        // map could contain e itself
                        if (distances.containsKey(e)) {
                            numElem --;
                        }
                        return new Pair<>(e, numElem/sum);
                    } else if (kind.equalsIgnoreCase("vertex") || kind.equalsIgnoreCase("both")) {
                        int cc_id = -1;
                        // a vertex can belong to many s-ccs; its s-centrality
                        // value is the max among all the centralities 
                        // (the smallest the value, the more central the vertex)
                        double approxCent = 0.;
                        
                        for (int e1 : vMap.getOrDefault(e, Sets.newHashSet())) {
                            // to determine if there is at least a hyperedge
                            // including vertex e and with size >= s
                            int thisID = oracle.getIdOfSCC(e1, s);
                            if (thisID == -1) {
                                continue;
                            }
                            cc_id = Math.max(cc_id, thisID);
                            // check if hyperedge e1 belongs to a small component
                            Map<Integer, Integer> thisDistances = oracle.getOracle(s).getLabel(e1);
                            if (thisDistances.isEmpty()) {
                                int cc_size = oracle.getSizeOf(s, thisID);
                                approxCent = Math.max(approxCent, oracle.getApproxDistance(cc_size, 1));
                            } else {
                                // otherwise compute its s-centrality
                                double sum = thisDistances.values().stream().mapToInt(i -> i).sum();
                                int numElem = thisDistances.size() - 1;
                                // map could contain e itself
                                if (thisDistances.containsKey(e)) {
                                    numElem --;
                                }
                                approxCent = Math.max(approxCent, numElem/sum);
                            }
                        }
                        if (cc_id == -1) {
                            return new Pair<>(e, 0.);
                        }
                        return new Pair<Integer, Double>(e, approxCent);
                    }
                    throw new IllegalArgumentException("Kind " + kind + " not supported.");
                })
                .collect(Collectors.toMap(e -> e.getValue0(), e -> e.getValue1()));
    }
    
    
    /**
     * Finds the approximate s-centrality for the query elements, 
     * using a distance oracle.
     * 
     * @param vMap for each vertex, the set of hyperedges including that vertex
     * @param oracle distance oracle
     * @param queries query elements
     * @param maxD
     * @param k number of closest neighbors to find
     * @param kind kind of distance to compute, among "edge" (edge to edge),
     * "vertex" (vertex to vertex), and "both" (vertex to edge)
     * @return for each s up to maxD, top-k elements s-reachable from each query, 
     * computed using the distance oracle
     * @throws IOException 
     */
    public static Map<Integer, int[][]> findATopKReachable(
            Map<Integer, Set<Integer>> vMap,
            DistanceOracle oracle,
            List<Integer> queries,
            int maxD,
            int k,
            String kind) throws IOException {
        
        Map<Integer, Set<Integer>> invMap = Maps.newHashMap();
        if(kind.equalsIgnoreCase("vertex")) {
            vMap.entrySet()
                    .stream()
                    .forEach(en -> {
                        for(int e : en.getValue()) {
                            Set<Integer> tmp = invMap.getOrDefault(e, Sets.newHashSet());
                            tmp.add(en.getKey());
                            invMap.put(e, tmp);
                        }
                    });
        }
        
        Map<Integer, int[][]> topKPerS = Maps.newHashMap();
        for (int s = 1; s <= maxD; s++) {
            int[][] topKPerQuery = new int[queries.size()][k];
            final int thisS = s;
            Map<Integer, int[]> reachables = queries.stream()
                .parallel()
                .map(q -> {
                    Map<Integer, Integer> reached = Maps.newHashMap();
                    int[] sortedReached = new int[k];
                    if (kind.equalsIgnoreCase("edge")) {
                        reached = oracle.getOracle(thisS).getLabel(q);
                    } else {
                        for (int e1 : vMap.getOrDefault(q, Sets.newHashSet())) {
                            Map<Integer, Integer> reachedFromE = oracle.getOracle(thisS).getLabel(e1);
                            // find s-distance from e to all the reachable vertices
                            for (Map.Entry<Integer, Integer> entry : reachedFromE.entrySet()) {
                                if (kind.equalsIgnoreCase("vertex")) {
                                    for (int ngb : invMap.get(entry.getKey())) {
                                        reached.put(ngb, 
                                                Math.min(reached.getOrDefault(ngb, Integer.MAX_VALUE), 
                                                        entry.getValue()));
                                    }
                                } else {
                                    reached.put(entry.getKey(), 
                                            Math.min(reached.getOrDefault(entry.getKey(), Integer.MAX_VALUE), 
                                                    entry.getValue()));
                                }
                            }
                        }
                    }
                    List<Map.Entry<Integer, Integer>> topKReached = Lists.newArrayList(reached.entrySet());
                    Collections.sort(topKReached, 
                            (Map.Entry<Integer, Integer> e1, Map.Entry<Integer, Integer> e2) -> {
                                if (e1.getValue().equals(e2.getValue())) {
                                    return Integer.compare(e1.getKey(), e2.getKey());
                                }
                                return Integer.compare(e1.getValue(), e2.getValue());
                            });
                    int maxK = Math.min(k, topKReached.size());
                    for (int i = 0; i < maxK; i++) {
                        sortedReached[i] = topKReached.get(i).getKey();
                    }
                    for (int i = maxK; i < k; i++) {
                        sortedReached[i] = -1;
                    }
                    return new Pair<Integer, int[]>(q, sortedReached);
                })
                .collect(Collectors.toMap(e -> e.getValue0(), e -> e.getValue1()));
            for (int i = 0; i < queries.size(); i++) {
                topKPerQuery[i] = reachables.get(queries.get(i));
            }
            topKPerS.put(thisS, topKPerQuery);
        
        }
        return topKPerS;
    }

    /**
     * Optimize lower and upper bounds to the s-distances, based on two observations.
     * 1. ub_s = min(ub_s, lb_{s+1})
     * 2. lb_s = max(lb_s, lb_{s-1})
     * 
     * @param profiles distance profiles
     * @return refined distance profiles
     */
    public static Map<Pair<Integer, Integer>, DistanceProfile> refineDistanceProfiles(
            Map<Pair<Integer, Integer>, DistanceProfile> profiles) {
        
        Map<Pair<Integer, Integer>, DistanceProfile> refined = Maps.newHashMap();
        profiles.entrySet().stream().forEach(profile -> {
            Map<Integer, Triplet<Double, Double, Double>> approxDist = profile.getValue().getDistanceProfile();
            int maxS = Collections.max(approxDist.keySet());
            
            for (int s = 1; s <= maxS; s++) {
                Triplet<Double, Double, Double> entryS = approxDist.get(s);
                // improved upper-bound
                double candUB;
                if (!approxDist.containsKey(s+1) || approxDist.get(s+1).getValue1() == -1) {
                    candUB = Double.MAX_VALUE;
                } else {
                    candUB = approxDist.get(s+1).getValue1();
                }
                entryS = entryS.setAt1(Math.min(entryS.getValue1(), candUB));
                // improved lower-bound
                if (s > 1) {
                    double candLB;
                    if (!approxDist.containsKey(s-1)) {
                        candLB = -1;
                    } else {
                        candLB = approxDist.get(s-1).getValue0();
                    }
                    entryS = entryS.setAt0(Math.max(entryS.getValue0(), candLB));
                }
                approxDist.put(s, entryS);
            }
            int p = profile.getKey().getValue0();
            int q = profile.getKey().getValue1();
            refined.put(profile.getKey(), new DistanceProfile(p, q, approxDist));
        });
        return refined;
    }
    
    /**
     * 
     * @param labels map with the label of each element
     * @param numLabels number of labels to sample
     * @return a sample of numQueries random elements for 5 random tags
     * @throws IOException 
     */
    public static List<List<Integer>> sampleByTag(Map<Integer, String> labels, int numLabels) throws IOException {
        // filter labels with at least numQueries elements
        List<Map.Entry<String, Long>> labelsOcc = labels.entrySet()
                .stream()
                .collect(Collectors.groupingBy(e -> e.getValue(), Collectors.counting()))
                .entrySet()
                .stream()
                .collect(Collectors.toList());
        // sample 5 labels at random
        RandomSamplingCollector<String> labelCollector = LiLSampling.collector(
                numLabels, 
                new Random(Settings.seed));
        List<String> selectableLabels = labelsOcc
                .stream()
                .filter(e -> e.getValue() >= Settings.numQueries)
                .map(e -> e.getKey())
                .collect(labelCollector)
                .stream()
                .collect(Collectors.toList());
        // sample numQueries elements for each label
        Map<String, Set<Integer>> itemsPerLabel = labels.entrySet()
                .stream()
                .collect(Collectors.groupingBy(e -> e.getValue(), 
                        Collectors.mapping(e -> e.getKey(), Collectors.toSet())));
        RandomSamplingCollector<Integer> collector = LiLSampling.collector(
                Settings.numQueries, 
                new Random(Settings.seed));
        return selectableLabels
                .stream()
                .map(label -> itemsPerLabel.get(label)
                        .stream()
                        .collect(collector)
                        .stream()
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }
    
    /**
     * Creates a distance oracle for the given hypergraph, or loads it from disk.
     * 
     * @param graph hypergraph
     * @param maxD max overlap size s to consider
     * @return distance oracle for the hypergraph
     * @throws FileNotFoundException 
     */
    public static Pair<DistanceOracle, Long> getDistanceOracle(HyperGraph graph, int maxD) throws FileNotFoundException, IOException {
        // oracle settings
        double[] importance;
        if (Settings.landmarkAssignment.equals("ranking")) {
            importance = new double[]{Settings.alpha, Settings.alpha, Settings.beta, 1.};
        } else {
            importance = new double[]{Settings.alpha, Settings.beta};
        }
        DistanceOracle oracle = new DistanceOracle();
        ConnectedComponents CCS;
        boolean computeFromScratch = !Settings.store;
        if (Settings.store) {
            try {
                oracle = Reader.readOracle();
            } catch (FileNotFoundException ex) {
                computeFromScratch = true;
                System.out.println("oracle file not found.");
            }
             try {
                // initialize structures
                CCS = Reader.readConnectedComponents();
                if (!Settings.isApproximate) {
                    graph.initializeNeighboursFromCandidates(CCS, maxD, Settings.lb);
                }
                CCS.clearStructures();
            } catch (FileNotFoundException ex) {
                System.out.println("connected components file not found.");
            }
        }
        StopWatch watch = new StopWatch();
        long creationTime = 0L;
        if (computeFromScratch) {
            if (graph == null) {
                graph = Reader.loadGraph(Settings.dataFolder + Settings.dataFile, false);
                System.out.println("graph loaded.");
            }
            watch.start();
            // create oracle
            oracle.populateOracles(
                    graph, Settings.landmarkSelection, 
                    Settings.landmarkAssignment, maxD, 
                    Settings.numLandmarks, Settings.lb, importance, 
                    false, Settings.seed);
            creationTime = watch.getElapsedTime();
            System.out.println("oracle created in (s) " + creationTime);
            if (Settings.store) {
                Writer.writeOracle(oracle);
                System.out.println("oracle written on disk.");
            }
        }
        return new Pair<>(oracle, creationTime);
    }
    
}
