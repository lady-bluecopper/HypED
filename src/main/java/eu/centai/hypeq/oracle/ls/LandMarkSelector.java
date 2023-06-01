package eu.centai.hypeq.oracle.ls;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.centai.hyped.cc.ConnectedComponents;
import eu.centai.hypeq.oracle.ls.ra.BioConsert;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.Utils;
import gr.james.sampling.LiLSampling;
import gr.james.sampling.RandomSamplingCollector;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.javatuples.Pair;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

/**
 *
 * @author giulia
 */
public class LandMarkSelector {
    
    HyperGraph graph;
    Random rand;
    String method;
    Map<Integer, List<Set<Integer>>> pathsPerS; // used only by baseline
    
    /**
     * 
     * @param graph hypergraph
     * @param method strategy used to select the landmarks in the hypergraph
     * @param seed seed for reproducibility
     */
    public LandMarkSelector(HyperGraph graph, String method, int seed) {
        this.graph = graph;
        this.rand = new Random(seed);
        this.method = method;
    }
    
    /**
     * CCS-SW: Select landmarks for all the s-ccs at once.
     * 
     * @param CCS connected components of the hypergraph
     * @param budget desired oracle size
     * @param lb no landmark is assigned to s-ccs with size not greater than lb
     * @param importance if the strategy is ranking, it includes the importance 
     * factor for each ranking of the s-ccs; otherwise it includes the alpha and 
     * beta values for computing the probability of a s-cc to be selected.
     * Note that alpha + beta must not exceed 1.
     * @param assMethod strategy to assign landmarks to s-ccs
     * @return for each s, the landmarks selected
     */
    public Map<Integer, Set<Integer>> selectAllLandmarks(
            ConnectedComponents CCS, 
            int budget, 
            int lb, 
            double[] importance,
            String assMethod) {
       
        // initialize structures needed to assign the landmarks
        int totalS = CCS.size();
        int[] numCCPerS = new int[totalS];
                IntStream.range(0, totalS)
                .boxed()
                .forEach(s -> numCCPerS[s] = CCS.getSCCs(s+1).size());
        int totalCCS = Arrays.stream(numCCPerS).sum();
        int[] cumSum = Utils.cumSum(numCCPerS);
        // sizes of the s-ccs with size > lb
        int[] allCCs = new int[totalCCS];
        // sa value of the s-ccs with size > lb
        int[] allSs = new int[totalCCS];
        // number of vertices of the s-ccs with size > lb
        int[] allVs = new int[totalCCS];
        IntStream.range(0, totalS)
                .parallel()
                .forEach(s -> {
                    IntStream.range(0, CCS.getSCCs(s+1).size())
                        .forEach(i -> {
                            int size = CCS.getSCCs(s+1).get(i).size();
                            int pos = 0;
                            if (s > 0) {
                                pos = cumSum[s-1];
                            }
                            if (size > lb) {
                                allCCs[pos + i] = size;
                                int numV = CCS.getSCCs(s+1).get(i).stream()
                                    .flatMap(id -> graph.getVerticesOf(id).stream())
                                    .collect(Collectors.toSet())
                                    .size();
                                allVs[pos + i] = numV;
                            }
                            allSs[pos + i] = s + 1;
                        });
                });
        
        if (assMethod.equalsIgnoreCase("ranking")) {
            System.out.println("starting search using ranking strategy");
            return selectLandmarksUsingRA(allCCs, allSs, allVs, cumSum, CCS, budget, lb, importance);
        }
        // ELSE: method is sampling
        System.out.println("starting search using sampling strategy");
        LandMarkAssigner la = new LandMarkAssigner(rand);
        Pair<int[], int[]> solution = la.assignLandmarksAndSizes(allCCs, allSs, 
                allVs, budget, 
                importance[0], 
                importance[1], 
                method);
        int[] sampleSizes = solution.getValue0();
        int[] landmarks = solution.getValue1();
        return IntStream
                .range(0, totalS)
                .boxed()
                .map(s -> {
                    int start = 0;
                    if (s > 0) {
                        start = cumSum[s-1];
                    }
                    Set<Integer> selected = selectLandmarks(sampleSizes, 
                            landmarks, 
                            start, 
                            CCS.getSCCs(s+1), 
                            s+1); 
                    return new Pair<Integer, Set<Integer>>(s+1, selected);
                })
                .collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));
    }
    
    /**
     * Baseline: Select landmarks according to the strategy specified.
     * This method does not exploit the s-connected components.
     *
     * @param alreadySelected landmarks selected in previous iterations
     * @param numLandmarks number of landmarks to select (if not ccBased)
     * @param s min overlap size
     * @return set of landmarks selected
     */
    public Set<Integer> selectLandmarks(Set<Integer> alreadySelected, int numLandmarks, int s) {
        Set<Integer> candidates = Sets.newHashSet(graph.getEdgesWithMinSize(s));
        candidates.removeAll(alreadySelected);
        if (candidates.isEmpty()) {
            return Sets.newHashSet();
        }
        //(sample size, number of landmarks to extract, candidates)
        int actualNumLands = Math.min(candidates.size(), numLandmarks);
        int sampleSize = Math.max(1, (int) (candidates.size() * Settings.samplePerc));
        return selectionInCC(candidates, actualNumLands, sampleSize, s);
    }
    
    /**
     * CCS-IS: Select landmarks according to the strategy specified.
     * This method exploits the s-connected components.
     *
     * @param ccs s-connected components
     * @param budget max oracle size
     * @param lb no landmark is assigned to s-ccs with size not greater than lb
     * @param importance importance factor for selection
     * @param s max overlap size
     * @return set of landmarks selected
     */
    public Set<Integer> selectLandmarksPerCC(List<List<Integer>> ccs, 
            int budget, 
            int lb, 
            double[] importance, 
            int s) {
        
        // check if we actually have s-ccs
        int numCCs = ccs.size();
        if (ccs.isEmpty()) {
            Sets.newHashSet();
        }
        // create meta-structures
        int[] ccsSizes = new int[numCCs];
        int[] ccsVs = new int[numCCs];
        // dummy vector; it is not used since beta = 0 in this case
        int[] ccsS = new int[numCCs];
        IntStream.range(0, numCCs)
                .parallel()
                .forEach(i -> {
                    int size = ccs.get(i).size();
                    if (size > lb) {
                        ccsSizes[i] = size;
                        int numV = ccs.get(i).stream()
                            .flatMap(id -> graph.getVerticesOf(id).stream())
                            .collect(Collectors.toSet())
                            .size();
                        ccsVs[i] = numV;
                    }
                });
        // assign landmarks to s-connected components
        LandMarkAssigner la = new LandMarkAssigner(rand);
        System.out.println("starting search using sampling strategy");
        Pair<int[], int[]> solution = la.assignLandmarksAndSizes(ccsSizes, ccsS, 
                ccsVs, budget, importance[0] + importance[1]/2, 0., method);
        int[] sampleSizes = solution.getValue0();
        int[] landmarks = solution.getValue1();
        return selectLandmarks(sampleSizes, landmarks, 0, ccs, s);
    }
    
    /**
     * Given the number of landmarks to sample from each s-connected component, 
     * select them according to the strategy.
     * 
     * @param sampleSizePerCC number of hyperedges to sample for each s-connected component
     * @param numLandmarksPerCC number of landmarks to sample for each s-connected component
     * @param start start id of the portions of the vectors that must be considered
     * @param ccs s-connected components
     * @param s min overlap size
     * @return set of landmarks selected
     */
    public Set<Integer> selectLandmarks(
            int[] sampleSizePerCC, 
            int[] numLandmarksPerCC, 
            int start,
            List<List<Integer>> ccs, 
            int s) {
        
        if (ccs.isEmpty()) {
            return Sets.newHashSet();
        }
        return IntStream.range(0, ccs.size())
                .parallel()
                .boxed()
                .flatMap(i -> selectionInCC(ccs.get(i), numLandmarksPerCC[start + i], sampleSizePerCC[start + i], s).stream())
                .collect(Collectors.toSet());
    }
    
    /**
     * This method assign one landmark at a time, until the budget is reached.When an additional landmark is assigned to a s-cc, the landmark is selected 
 right away, and the set of candidates is updated.
     * Landmarks are assigned according to an aggregate ranking of the s-ccs,
 which is updated after each assignment.
     * 
     * @param allCCs sizes of the s-ccs with size > lb
     * @param allSs  s value of the s-ccs with size > lb
     * @param allVs number of vertices of the s-ccs with size > lb
     * @param cumSum vector used to get the position of a s-cc in the other vectors
     * @param CCS s-connected components, for all s
     * @param budget max oracle size in terms of distance pairs to store
     * @param lb min size of a s-cc for which a landmark can be assigned
     * @param importance importance factor for each ranking of the s-ccs
     * @return for each s, the set of landmarks selected
     */
    public Map<Integer, Set<Integer>> selectLandmarksUsingRA(
            int[] allCCs, 
            int[] allSs,
            int[] allVs,
            int[] cumSum,
            ConnectedComponents CCS,
            int budget, 
            int lb,
            double[] importance) {
        
        int total = allCCs.length;
        // for each s-cc, store the set of candidate landmarks
        IntOpenHashSet[] candsMap = new IntOpenHashSet[total];
        // if method is betweenness/bestcover, we need to store the paths 
        // to which the candidate landmarks belong
        Map<Integer, Set<Integer>>[] pathsInSample = new Map[total];
        // create rankings
        // -- ranking by size
        Int2ObjectOpenHashMap<IntOpenHashSet> tmp1 = new Int2ObjectOpenHashMap();
        // -- ranking by vertex size
        Int2ObjectOpenHashMap<IntOpenHashSet> tmp2 = new Int2ObjectOpenHashMap();
        // -- ranking by s value
        Int2ObjectOpenHashMap<IntOpenHashSet> tmp3 = new Int2ObjectOpenHashMap();
        // -- ranking by landmarks (includes only the selectable)
        Int2ObjectOpenHashMap<IntOpenHashSet> tmp4 = new Int2ObjectOpenHashMap();
        tmp4.put(0, new IntOpenHashSet());
        
        for (int i = 0; i < allCCs.length; i++) {
            if (allCCs[i] > 0) {
                if (!tmp1.containsKey(allCCs[i])) {
                    tmp1.put(allCCs[i], new IntOpenHashSet());
                }
                tmp1.get(allCCs[i]).add(i);
                if (!tmp2.containsKey(allVs[i])) {
                    tmp2.put(allVs[i], new IntOpenHashSet());
                }
                tmp2.get(allVs[i]).add(i);
                if (!tmp3.containsKey(allSs[i])) {
                    tmp3.put(allSs[i], new IntOpenHashSet());
                }
                tmp3.get(allSs[i]).add(i);
                tmp4.get(0).add(i);
            }
        }
        // candidates for each connected component
        IntStream.range(0, total)
                .filter(i -> allCCs[i] > 0)
                .forEach(i -> {
                    int s = getSFromPos(cumSum, i);
                    int drift = (s > 1) ? cumSum[s-2] : 0;
                    candsMap[i] = new IntOpenHashSet(CCS.getSCCs(s).get(i - drift));
                });
        // initial ranking
        List<List<Integer>> med = BioConsert.computeMedianRanking(tmp1, tmp2, tmp3, tmp4, importance);
        System.out.println("initial ranking found");
        // keep track of estimate oracle size 
        int estOracleSize = 0;
        // keep track of landmarks selected
        Map<Integer, Set<Integer>> landmarksAssigned = Maps.newHashMap();
        // select landmarks until we reach the budget size
        int s, oldLands, sel;
        while (estOracleSize < budget) {
            // take one of the best ranked s-ccs, at random
            List<Integer> best = med.get(0);
            // s-cc selected
            sel = best.get(rand.nextInt(best.size()));
            s = getSFromPos(cumSum, sel);
            // get candidates and landmarks selected for candSCC
            Set<Integer> alreadyAssigned = landmarksAssigned.getOrDefault(sel, Sets.newHashSet());
            IntOpenHashSet candidates = candsMap[sel];
            Map<Integer, Set<Integer>> paths = pathsInSample[sel];
            if (paths == null) {
                paths = Maps.newHashMap();
            }
            oldLands = alreadyAssigned.size();
            // select a landmark for it
            selectionInCC(paths, candidates, alreadyAssigned, s);
            // update info
            estOracleSize += allCCs[sel];
            landmarksAssigned.put(sel, alreadyAssigned);
            candsMap[sel] = candidates;
            pathsInSample[sel] = paths;
            // update landmark ranking
            tmp4.get(oldLands).remove(sel);
            if (tmp4.get(oldLands).isEmpty()) {
                tmp4.remove(oldLands);
            }
            // update size ranking if candSCC has no more candidates
            if (candidates.isEmpty()) {
                tmp1.get(allCCs[sel]).remove(sel);
                if (tmp1.get(allCCs[sel]).isEmpty()) {
                    tmp1.remove(allCCs[sel]);
                }
                tmp2.get(allVs[sel]).remove(sel);
                if (tmp2.get(allVs[sel]).isEmpty()) {
                    tmp2.remove(allVs[sel]);
                }
                tmp3.get(s).remove(sel);
                if (tmp3.get(s).isEmpty()) {
                    tmp3.remove(s);
                }
            } else {
                int newLands = alreadyAssigned.size();
                if (!tmp4.containsKey(newLands)) {
                    tmp4.put(newLands, new IntOpenHashSet());
                }
                tmp4.get(newLands).add(sel);
            }
            // stop the search if there are no candidate left
            if (tmp1.isEmpty()) {
                break;
            }
            // update median ranking
            med = BioConsert.computeMedianRanking(tmp1, tmp2, tmp3, tmp4, importance);
        }
        // return, for each s, the set of landmarks selected for all the s-ccs
        Map<Integer, Set<Integer>> landmarks = Maps.newHashMap();
        IntStream.range(0, total)
                .filter(i -> allCCs[i] > 0)
                .forEach(id -> {
                    int sid = getSFromPos(cumSum, id);
                    Set<Integer> tmpSet = landmarks.getOrDefault(sid, Sets.newHashSet());
                    tmpSet.addAll(landmarksAssigned.getOrDefault(id, Sets.newHashSet()));
                    landmarks.put(sid, tmpSet);
                });
        return landmarks;
    }
    
    /**
     * 
     * @param cumSum vector used to get the position of a s-cc in the other vectors
     * @param pos position in the vector
     * @return s value associated to the component at position pos in the vectors
     */
    private int getSFromPos(int[] cumSum, int pos) {
        for (int i = 0; i < cumSum.length; i++) {
            if (pos < cumSum[i]) {
                return i+1;
            }
        }
        return -1;
    }
    
    /**
     * Select multiple landmarks among the candidates, according to the strategy.
     * 
     * @param candidates set of candidate hyperedge ids
     * @param numLandmarks number of hyperedges to select
     * @param sampleSize number of hyperedges to sample to find paths
     * (used only by bestcover and between methods)
     * @param s min size
     * @return hyperedges selected according to the strategy specified at creation time
     */
    private Set<Integer> selectionInCC(Collection<Integer> candidates, int numLandmarks, int sampleSize, int s) {
        if (candidates.size() == numLandmarks) {
            return Sets.newHashSet(candidates);
        }
        if (method.equalsIgnoreCase("degree")) {
            return degreeSelection(candidates, numLandmarks, s);
        } 
        if (method.equalsIgnoreCase("farthest")) {
            return farthestSelection(candidates, numLandmarks, s);
        }
        if (method.equalsIgnoreCase("bestcover")) {
            return bestCoverSelection(candidates, numLandmarks, sampleSize, s);
        }
        if (method.equalsIgnoreCase("between")) {
            return betweennessSelection(candidates, numLandmarks, sampleSize, s);
        }
        return randomSelection(candidates, numLandmarks);
    }
    
    /**
     * Select a landmarks among the candidates, given a set of landmarks already 
     * selected.
     * 
     * @param pathsInSample for each candidate, paths to which it belongs
     * @param candidates set of candidate hyperedge ids
     * @param currLandmarks set of landmarks already selected
     * @param s min overlap size
     */
    private void selectionInCC(Map<Integer, Set<Integer>> pathsInSample, 
            Set<Integer> candidates, 
            Set<Integer> currLandmarks, 
            int s) {
        if (method.equalsIgnoreCase("degree")) {
            degreeSelection(candidates, currLandmarks, s);
        } else if (method.equalsIgnoreCase("farthest")) {
            farthestSelection(candidates, currLandmarks, s);
        } else if (method.equalsIgnoreCase("bestcover")) {
            bestCoverSelection(pathsInSample, candidates, currLandmarks, s);
        } else if (method.equalsIgnoreCase("between")) {
            betweennessSelection(pathsInSample, candidates, currLandmarks, s);
        } else {
            randomSelection(candidates, currLandmarks);
        }
    }
    
    /**
     * Select landmarks uniformly at random.
     * 
     * @param candidates set of candidate hyperedge ids
     * @param numLandmarks number of hyperedges to select
     * @return set of random hyperedges
     */
    private Set<Integer> randomSelection(Collection<Integer> candidates, int numLandmarks) {
        List<Integer> canList = Lists.newArrayList(candidates);
        Collections.shuffle(canList, rand);
        Set<Integer> landmarks = Sets.newHashSet();
        for (int i = 0; i < numLandmarks; i++) {
            landmarks.add(canList.get(i));
        }
        return landmarks;
        
//        RandomSamplingCollector<Integer> landmarkCollector = LiLSampling.collector(
//                numLandmarks, 
//                rand);
//        return candidates
//                .stream()
//                .collect(landmarkCollector)
//                .stream()
//                .collect(Collectors.toSet());
    }
    
    /**
     * Select a new landmark uniformly at random among the candidates.
     * 
     * @param candidates set of candidate hyperedge ids
     * @param currLandmarks set of landmarks already selected
     */
    private void randomSelection(Set<Integer> candidates, Set<Integer> currLandmarks) {
        List<Integer> canList = Lists.newArrayList(candidates);
        int newLand = canList.get(rand.nextInt(canList.size()));
        currLandmarks.add(newLand);
        candidates.remove(newLand);
    }
    
    /**
     * Select landmarks among hyperedges with largest degrees.
     * 
     * @param candidates set of candidate hyperedge ids
     * @param numLandmarks number of hyperedges to select
     * @param s min size
     * @return hyperedges selected according to the degree strategy
     */
    private Set<Integer> degreeSelection(Collection<Integer> candidates, int numLandmarks, int s) {
        Set<Integer> landmarks = Sets.newHashSet();
        if (numLandmarks == 0) {
            return landmarks;
        }
        // hyperedges currently selected
        PriorityQueue<Pair<Integer, Integer>> selection = new PriorityQueue(
                new Comparator<Pair<Integer, Integer>>(){
                    @Override
                    public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                        return Integer.compare(o1.getValue1(), o2.getValue1());
                    }
                });
        candidates.stream().forEach(e -> {
            int numNeig = graph.getNumSNeighborsOf(e, s);
            if (selection.size() < numLandmarks) {
                selection.add(new Pair<>(e, numNeig));
            } else if (numNeig > selection.peek().getValue1()) {
                selection.poll();
                selection.add(new Pair<>(e, numNeig));
            }
        });
        selection.forEach(p -> landmarks.add(p.getValue0()));
        return landmarks;
    }
    
    /**
     * Select a landmark among hyperedges with largest degrees.
     * 
     * @param candidates set of candidate hyperedge ids
     * @param currLandmarks set of landmarks already selected
     * @param s min size
     * @return hyperedges selected according to the degree strategy
     */
    private void degreeSelection(Set<Integer> candidates, Set<Integer> currLandmarks, int s) {
        List<Pair<Integer, Integer>> candList = candidates.stream()
                .map(c -> new Pair<Integer, Integer>(c, graph.getNumSNeighborsOf(c, s)))
                .collect(Collectors.toList());
        Collections.sort(candList, new Comparator<Pair<Integer, Integer>>(){
                    @Override
                    public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                        return - Integer.compare(o1.getValue1(), o2.getValue1());
                    }
                });
        int newL = candList.get(0).getValue0();
        currLandmarks.add(newL);
        candidates.remove(newL);
    }
    
    /**
     * Select landmarks most distant from each others.
     * 
     * @param candidates set of candidate hyperedge ids
     * @param numLandmarks number of hyperedges to select
     * @param s min size
     * @return hyperedges selected according to the farthest strategy
     */
    private Set<Integer> farthestSelection(Collection<Integer> candidates, int numLandmarks, int s) {
        Set<Integer> landmarks = Sets.newHashSet();
        if (numLandmarks == 0) {
            return landmarks;
        }
        // distances from landmarks selected
        Map<Integer, Map<Integer, Integer>> Ldist = Maps.newHashMap();
        // hyperedges reachable from any landmark selected
        Set<Integer> reachable = Sets.newHashSet();
        List<Integer> candList = Lists.newArrayList(candidates);
        Collections.shuffle(candList, rand);
        Map<Integer, Integer> candMap = Maps.newHashMap();
        BitSet selectable = new BitSet(candidates.size());
        for (int i = 0; i < candidates.size(); i++) {
            candMap.put(candList.get(i), i);
            if (graph.getEdgeDegree(candList.get(i), s) > 0) {
                selectable.set(i, true);
            } else {
                reachable.add(candList.get(i));
            }
        }
        if (selectable.isEmpty()) {
            return randomSelection(candidates, numLandmarks);
        }
        // first landmark is random
        int b = selectable.nextSetBit(0);
        int nextL = candList.get(b);
        landmarks.add(nextL);
        selectable.set(b, false);
        while (landmarks.size() < numLandmarks) {
            // find distances from nextL
            if (!Ldist.containsKey(nextL)) {
                Ldist.put(nextL, graph.findDistancesFrom(nextL, s));
                reachable.addAll(Ldist.get(nextL).keySet());
                
                Ldist.get(nextL).keySet().stream()
                        .filter(k -> candMap.containsKey(k))
                        .forEach(k -> selectable.set(candMap.get(k), false));
            }
            // find farthest hyperedge
            // if some hyperedge is not reachable from any landmark
            // with probability 1/2 pick an unreached landmark
            double pick = rand.nextDouble();
            int max = -1;
            if (reachable.size() < candidates.size() && 
                    pick < 1 - 1. * reachable.size() / candidates.size() &&
                    !selectable.isEmpty()) {
                nextL = candList.get(selectable.nextSetBit(0));
                // ensures we add nextL to the set of landmarks
                max = 0;
            // otherwise select the farthest
            } else {
                for (int e : reachable) {
                    // already selected
                    if (landmarks.contains(e) || !candMap.containsKey(e)) {
                        continue;
                    }
                    int minD = Integer.MAX_VALUE;
                    for (Map<Integer, Integer> map : Ldist.values()) {
                        minD = Math.min(minD, map.getOrDefault(e, Integer.MAX_VALUE));
                    }
                    if (minD > max) {
                        nextL = e;
                        max = minD;
                    }
                }
            }
            if (max >= 0) {
                landmarks.add(nextL);
                selectable.set(candMap.get(nextL), false);
            }
        }
        return landmarks;
    }
    
    /**
     * Select the landmark most distant from the landmarks already selected.
     * 
     * @param candidates set of candidate hyperedge ids
     * @param currLandmarks set of landmarks already selected
     * @param s min size
     */
    private void farthestSelection(Set<Integer> candidates, Set<Integer> currLandmarks, int s) {
        // distances from landmarks selected
        Map<Integer, Map<Integer, Integer>> Ldist = Maps.newHashMap();
        currLandmarks.stream().forEach(l -> {
            Map<Integer, Integer> distances = graph.findDistancesFrom(l, s);
            distances.entrySet().stream().forEach(entry -> {
                if (candidates.contains(entry.getKey())) {
                    Map<Integer, Integer> tmp = Ldist.getOrDefault(entry.getKey(), Maps.newHashMap());
                    tmp.put(l, entry.getValue());
                    Ldist.put(entry.getKey(), tmp);
                }
            });
        });
        // find farthest hyperedge
        int max = 0;
        int newL = -1;
        for (int c : candidates) {
            int minD = Integer.MAX_VALUE;
            for (Map<Integer, Integer> map : Ldist.values()) {
                minD = Math.min(minD, map.getOrDefault(c, Integer.MAX_VALUE));
            }
            if (minD > max) {
                newL = c;
                max = minD;
            }
        }
        currLandmarks.add(newL);
        candidates.remove(newL);
    }
    
    /**
     * Select landmarks that cover most of the hyperpaths.
     * 
     * @param candidates set of candidate hyperedge ids
     * @param numLandmarks number of hyperedges to select
     * @param sampleSize number of hyperedges to sample to find paths
     * @param s min size
     * @return hyperedges selected according to the best cover strategy
     */
    private Set<Integer> bestCoverSelection(Collection<Integer> candidates, int numLandmarks, 
            int sampleSize, int s) {
        Set<Integer> landmarks = Sets.newHashSet();
        if (numLandmarks == 0) {
            return landmarks;
        }
        List<Set<Integer>> paths;
        if (pathsPerS == null || pathsPerS.getOrDefault(s, Lists.newArrayList()).isEmpty()) {
            // find distances between sampled hyperedges
            paths = findPathsInSample(candidates, sampleSize, s, false);
        } else {
            paths = pathsPerS.get(s);
        }
        // keeps track of the paths removed
        BitSet present = new BitSet(paths.size());
        present.set(0, paths.size(), true);
        // find the number of paths in which the hyperedge is present
        Map<Integer, Integer> counts = Maps.newHashMap();
        paths.stream().forEach(path -> {
            path.stream()
                    .filter(v -> candidates.contains(v))
                    .forEach(v -> counts.put(v, counts.getOrDefault(v, 0) + 1));
        });
        // while there are uncovered shortest paths or
        // we do not have enough landmarks
        while (!present.isEmpty() && !counts.isEmpty() && landmarks.size() < numLandmarks) {
            List<Map.Entry<Integer, Integer>> entries = Lists.newArrayList(counts.entrySet());
            Collections.sort(entries, 
                    (Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) -> 
                            - Integer.compare(o1.getValue(), o2.getValue()));
            // next landmark is the hyperedge most present in the remaining paths
            int nextLand = entries.get(0).getKey();
            landmarks.add(nextLand);
            counts.remove(nextLand);
            // remove paths in which the current landmark is present
            for (int p = present.nextSetBit(0); p >= 0; p = present.nextSetBit(p+1)) {
                if (paths.get(p).contains(nextLand)) {
                    paths.get(p).stream()
                            .filter(v -> candidates.contains(v) && !landmarks.contains(v))
                            .forEach(v -> counts.put(v, counts.get(v) - 1));
                    present.set(p, false);
                }
            }
        }
        for (int p = 0; p < paths.size(); p++) {
            if (!present.get(p)) {
                paths.remove(p);
            }
        }
        if (pathsPerS != null) {
            pathsPerS.put(s, paths);
        }
        int currentSize = numLandmarks - landmarks.size();
        if (currentSize == 0) {
            return landmarks;
        }
        // if we still need some landmarks,
        // select those with larger degree
        Set<Integer> nonSelCands = Sets.newHashSet(candidates);
        nonSelCands.removeAll(landmarks);
        landmarks.addAll(degreeSelection(nonSelCands, currentSize, s));
        return landmarks;
    }
    
    /**
     * Select a landmark that cover most of the hyperpaths in the sample.
     * 
     * @param pathsInSample for each candidate, paths to which it belongs
     * @param candidates set of candidate hyperedge ids
     * @param currLandmarks set of landmarks already selected
     * @param sampleSize number of hyperedges to sample to find paths
     * @param s min size
     */
    private void bestCoverSelection(Map<Integer, Set<Integer>> pathsInSample, 
            Set<Integer> candidates, 
            Set<Integer> currLandmarks, 
            int s) {
        // find distances between sampled hyperedges
        // if not already computed
        if (candidates.size() == 1) {
            int newL = candidates.iterator().next();
            currLandmarks.add(newL);
            candidates.remove(newL);
            return;
        }
        boolean noPaths = pathsInSample.entrySet().stream().allMatch(e -> e.getValue().isEmpty());
        if (pathsInSample.isEmpty() || noPaths) {
            List<Set<Integer>> paths = findPathsInSample(candidates, -1, s, false);
            for (int i = 0; i < paths.size(); i++) {
                final int id = i;
                paths.get(i).stream().forEach(h -> {
                    Set<Integer> tmp = pathsInSample.getOrDefault(h, Sets.newHashSet());
                    tmp.add(id);
                    pathsInSample.put(h, tmp);
                });
            }
        }
        // find the number of paths in which the hyperedge is present
        List<Map.Entry<Integer, Set<Integer>>> entries = Lists.newArrayList(pathsInSample.entrySet());
        Collections.sort(entries, 
                (Map.Entry<Integer, Set<Integer>> o1, Map.Entry<Integer, Set<Integer>> o2) -> 
                        - Integer.compare(o1.getValue().size(), o2.getValue().size()));
        // should never happens if we are within a s-cc
        if (pathsInSample.isEmpty()) {
            degreeSelection(candidates, currLandmarks, s);
            return;
        }
        // next landmark is the hyperedge most present in the paths
        int newL = entries.get(0).getKey();
        currLandmarks.add(newL);
        candidates.remove(newL);
        // remove paths in which the current landmark is present
        Set<Integer> toRemove = pathsInSample.get(newL);
        for (int k : pathsInSample.keySet()) {
            pathsInSample.get(k).removeAll(toRemove);
        }
        pathsInSample.remove(newL);
    }
    
    /**
     * Select landmarks with largest (approximate) betweenness centrality.
     * 
     * @param candidates set of candidate hyperedge ids
     * @param numLandmarks number of hyperedges to select
     * @param sampleSize number of hyperedges to sample to find paths
     * @param s min size
     * @return hyperedges selected according to the best cover strategy
     */
    private Set<Integer> betweennessSelection(Collection<Integer> candidates, int numLandmarks, 
            int sampleSize, int s) {
        Set<Integer> landmarks = Sets.newHashSet();
        if (numLandmarks == 0) {
            return landmarks;
        }
        // find distances between sampled hyperedges
        List<Set<Integer>> paths;
        if (pathsPerS == null || pathsPerS.getOrDefault(s, Lists.newArrayList()).isEmpty()) {
            paths = findPathsInSample(candidates, sampleSize, s, true);
        } else {
            paths = pathsPerS.get(s);
        }
        if (pathsPerS != null) {
            pathsPerS.put(s, paths);
        }
        // find the number of paths in which the hyperedge is present
        Map<Integer, Integer> counts = Maps.newHashMap();
        paths.stream().forEach(path -> {
            path.stream()
                    .filter(v -> candidates.contains(v))
                    .forEach(v -> counts.put(v, counts.getOrDefault(v, 0) + 1));
        });
        List<Map.Entry<Integer, Integer>> entries = Lists.newArrayList(counts.entrySet());
            Collections.sort(entries, (Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) -> 
                            - Integer.compare(o1.getValue(), o2.getValue()));
        // while we do not have enough landmarks
        int i = 0;
        while (i < entries.size() && landmarks.size() < numLandmarks) {
            // next landmark is the hyperedge with largest betweenness among the remaining ones
            int nextLand = entries.get(i).getKey();
            landmarks.add(nextLand);
            i += 1;
        }
        int currentSize = numLandmarks - landmarks.size();
        if (currentSize == 0) {
            return landmarks;
        }
        // if we still need some landmarks,
        // select those with larger degree
        Set<Integer> nonSelCands = candidates.stream()
                .filter(e -> !landmarks.contains(e))
                .collect(Collectors.toSet());
        landmarks.addAll(degreeSelection(nonSelCands, currentSize, s));
        return landmarks;
    }
    
    /**
     * Select a landmark with largest (approximate) betweenness centrality.
     * 
     * @param pathsInSample for each candidate, paths to which it belongs
     * @param candidates set of candidate hyperedge ids
     * @param currLandmarks set of landmarks already selected
     * @param sampleSize number of hyperedges to sample to find paths
     * @param s min size
     */
    private void betweennessSelection(Map<Integer, Set<Integer>> pathsInSample, 
            Set<Integer> candidates, 
            Set<Integer> currLandmarks, 
            int s) {
        // find distances between sampled hyperedges
        // if not already computed
        if (candidates.size() == 1) {
            int newL = candidates.iterator().next();
            currLandmarks.add(newL);
            candidates.remove(newL);
            return;
        }
        if (pathsInSample.isEmpty()) {
            List<Set<Integer>> paths = findPathsInSample(candidates, -1, s, true);
            for (int i = 0; i < paths.size(); i++) {
                final int id = i;
                paths.get(i).stream().forEach(h -> {
                    Set<Integer> tmp = pathsInSample.getOrDefault(h, Sets.newHashSet());
                    tmp.add(id);
                    pathsInSample.put(h, tmp);
                });
            }
        }
        // should never happens if we are within a s-cc
        if (pathsInSample.isEmpty()) {
            degreeSelection(candidates, currLandmarks, s);
            return;
        }
        // find the number of paths in which the hyperedge is present
        List<Map.Entry<Integer, Set<Integer>>> entries = Lists.newArrayList(pathsInSample.entrySet());
        Collections.sort(entries, 
                (Map.Entry<Integer, Set<Integer>> o1, Map.Entry<Integer, Set<Integer>> o2) -> 
                        - Integer.compare(o1.getValue().size(), o2.getValue().size()));
        // next landmark is the hyperedge most present in the paths
        int newL = entries.get(0).getKey();
        currLandmarks.add(newL);
        candidates.remove(newL);
        pathsInSample.remove(newL);
    }
    
    /**
     * 
     * @param candidates candidate hyperedges
     * @param sampleSize number of hyperedges to sample; -1 if the sample is within a component
     * @param s min overlap size
     * @param allPaths whether all the paths between the pairs of hyperedges should be
     * searched, or just one path
     * @return s-paths among pairs of hyperedges sampled from candidates.
     */
    private List<Set<Integer>> findPathsInSample(Collection<Integer> candidates, 
            int sampleSize,
            int s,
            boolean allPaths) {
        int candsSize = candidates.size();
        if (sampleSize == -1) {
            sampleSize = Math.max(2, (int) Math.ceil(Settings.samplePerc * candsSize));
        }
        List<Integer> candList = Lists.newArrayList(candidates);
        // sample pairs of hyperedges at random
        Set<Pair<Integer, Integer>> sample = Utils.samplePairs(candList, sampleSize, rand);
        if (allPaths) {
            return sample.parallelStream()
                    .flatMap(p -> graph.findAllPathsBetween(p.getValue0(), 
                            p.getValue1(), 
                            s, 
                            candsSize).stream())
                    .filter(p -> !p.isEmpty())
                    .collect(Collectors.toList());
        }
        return sample.parallelStream()
                .map(p -> graph.bidirectionalSPSearch(
                        p.getValue0(), 
                        p.getValue1(), 
                        s, 
                        candsSize))
                .filter(p -> !p.isEmpty())
                .collect(Collectors.toList());
    }
    
    /**
     * Initialize data structure used to store the paths found in the sample.
     * It avoids the recomputation of the paths if multiple iterations of the baseline
     * are needed.
     */
    public void initializeCache() {
        this.pathsPerS = Maps.newHashMap();
    }
    
}
