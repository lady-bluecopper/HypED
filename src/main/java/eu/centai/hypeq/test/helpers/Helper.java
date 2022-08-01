package eu.centai.hypeq.test.helpers;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import gr.james.sampling.LiLSampling;
import gr.james.sampling.RandomSamplingCollector;
import eu.centai.hyped.cc.ConnectedComponents;
import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Writer;
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
