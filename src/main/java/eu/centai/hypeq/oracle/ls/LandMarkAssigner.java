package eu.centai.hypeq.oracle.ls;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.Utils;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import org.javatuples.Pair;

/**
 *
 * @author giulia
 */
public class LandMarkAssigner {
    
    Random rand;
    
    /**
     * 
     * @param rnd for reproducibility
     */
    public LandMarkAssigner(Random rnd) {
        this.rand = rnd;
    }
    
    /**
     * Select a number of landmarks per s-connected component, 
     * for each s, given a budget.
     * If a component has size not greater than lb, we set its size to 0 in the
     * vector allCCs, so that we know we must ignore it.
     * 
     * @param allCCs sizes of the all the connected components
     * @param allSs s for which the connected component is a s-connected component
     * @param allVs number of vertices of all the connected components
     * @param budget max oracle size in terms of distance pairs to store
     * @param alpha importance factor for component size
     * @param beta importance factor for component s value
     * @param method strategy used to select the landmarks (used to determine if 
     * we need to find a sample size as well)
     * @return sample size and number of landmarks to select for each s-connected component
     */
    public Pair<int[], int[]> assignLandmarksAndSizes(
            int[] allCCs,
            int[] allSs,
            int[] allVs,
            int budget, 
            double alpha,
            double beta,
            String method) {
        
        int len = allSs.length;
        // we set the s value to 0 if the corresponding connected component
        // should not be selected
        int[] dummyS = new int[len];
        IntStream.range(0, len)
                .parallel()
                .filter(i -> allCCs[i] != 0)
                .forEach(i -> dummyS[i] = allSs[i]);
        int[] landmarks = selectNumLandmarksviaSampling(allCCs, dummyS, allVs, budget, alpha, beta);
        int[] sampleSizes = new int[landmarks.length];
        if (!method.equalsIgnoreCase("bestcover") && !method.equalsIgnoreCase("between")) {
            // determine number of pairs to sample
            int numEdges = Arrays.stream(allCCs).sum();
            int sampleSize = (int) (numEdges * Settings.samplePerc);
            sampleSizes = selectSampleSizes(allCCs, landmarks, sampleSize);
        }
        return new Pair<>(sampleSizes, landmarks);
    }
    
    /** 
     * Assigns a sample size to each s-connected component, 
     * proportional to its size, and only if landmarks have been assigned to it.
     * 
     * @param ccsSizes size of each s-connected component
     * @param landmarksAssigned number of landmarks assigned to each s-connected component
     * @param totalItems total number of items to select
     * @return sample size for each s-connected component
     */
    public int[] selectSampleSizes(
            int[] ccsSizes, 
            int[] landmarksAssigned,
            int totalItems) {
        // keep track of num of items assigned to s-connected components
        int assigned = 0;
        int numCCs = ccsSizes.length;
        int[] numItems = new int[numCCs];
        // if max size <= 2, I do not need to sample items
        int maxSize = Ints.max(ccsSizes);
        if (maxSize <= 2) {
            return numItems;
        }
        // the probability of selecting an item for a s-connected component is
        // proportional to its size
         // mapping of ids for ccs that can be actually selected
        Map<Integer, Integer> invMap = Maps.newHashMap();
        List<Double> probList = Lists.newArrayList();
        BitSet zeros = new BitSet(numCCs);
        double sum = IntStream.range(0, numCCs)
                .boxed()
                .filter(i -> landmarksAssigned[i] > 0)
                .mapToInt(i -> ccsSizes[i])
                .sum();
        // initialize probabilities
        IntStream.range(0, numCCs)
                .forEach(i -> {
                    if (landmarksAssigned[i] > 0) {
                        probList.add(ccsSizes[i] / sum);
                        zeros.set(i);
                        invMap.put(invMap.size(), i);
                    }
                });
        // no selectable connected component
        if (zeros.isEmpty()) {
            return numItems;
        }
        // cumsum to get probability ranges
        double[] probs = Utils.cumSum(probList);
        double p;
        int id, i;
        while (assigned < totalItems) {
            // select next cc
            p = rand.nextDouble();
            id = Arrays.binarySearch(probs, p);
            if (id < 0) {
                id = (id + 1) * -1;
            }
            i = invMap.get(id);
            if (numItems[i] == ccsSizes[i]) {
                zeros.set(i, false);
            } else if (numItems[i] < ccsSizes[i]) {
                numItems[i] += 1;
                assigned += 1;
            }
            if (zeros.isEmpty()) {
                return numItems;
            }
        }
        // we need at least a pair of hyperedges to be able to find paths
        // between sampled hyperedges
        for (i = 0; i < numItems.length; i++) {
            if (numItems[i] > 0) {
                numItems[i] = Math.max(numItems[i], 2);
            }
        }
        return numItems;
    }
    
    /** Assigns a number of landmarks per s-connected component, 
     * proportional to (i) its size, (ii) s, and (iii) the number of vertices.
     * 
     * @param ccsSizes sizes of all the s-connected components, for all s
     * @param ccsS to which s, each connected component is a s-connected component
     * @param ccsV number of vertices in each connected component
     * @param budget max size of the oracle to create
     * @param alpha importance factor for component size
     * @param beta importance factor for component s value
     * @return number of landmarks to select for each s-connected component
     */
    public int[] selectNumLandmarksviaSampling(
            int[] ccsSizes, 
            int[] ccsS, 
            int[] ccsV,
            int budget, 
            double alpha,
            double beta) {
        
        if (alpha + beta > 1) {
            throw new IllegalArgumentException(alpha + " + " + beta + " must be <= 1");
        }
        // keep track of num of landmarks assigned
        int estOracleSize = 0;
        int[] numItems = new int[ccsSizes.length];
        // if max size <= 2, I do not need landmarks
        int maxSize = Ints.max(ccsSizes);
        if (maxSize <= 2) {
            return numItems;
        }
        // budget may be lower than max reachable oracle size
        BigInteger maxReachableSize = BigInteger.ZERO;
        BigInteger B = BigInteger.valueOf(budget);
        for (int size : ccsSizes) {
            BigInteger sx = BigInteger.valueOf(size);
            // handle int overflow
            if (sx.compareTo(B) > 0) {
                maxReachableSize = sx;
                break;
            }
            maxReachableSize = maxReachableSize.add(sx.multiply(sx));
            if (maxReachableSize.compareTo(B) > 0) {
                break;
            }
        }
        if (maxReachableSize.compareTo(B) <= 0) {
            System.out.println("Budget is too large: " + maxReachableSize.toString());
            return ccsSizes;
        }
        // assign items at random
        // the probability of selecting an item for a connected component is
        // proportional to its size and inversely proportional to s
        BitSet zeros = new BitSet(ccsSizes.length);
        double sumSizes = Arrays.stream(ccsSizes).sum();
        double sumS = Math.max(Arrays.stream(ccsS).sum(), 1);
        double sumV = Arrays.stream(ccsV).sum();
        // mapping of ids for ccs that can be actually selected
        Map<Integer, Integer> invMap = Maps.newHashMap();
        List<Double> probList = Lists.newArrayList();
        // initialize probabilities
        IntStream.range(0, ccsSizes.length)
                .forEach(i -> {
                    // if the connected component  
                    // (i) is larger
                    // (ii) has higher s
                    // (iii) has more vertices
                    // we assign larger probability
                    double p = alpha * ccsSizes[i] / sumSizes + 
                            beta * ccsS[i] / sumS + 
                            (1 - alpha - beta) * ccsV[i] / sumV;
                    if (p > 0) {
                        zeros.set(i);
                        probList.add(p);
                        invMap.put(invMap.size(), i);
                    }
                });
        // no selectable connected component
        if (zeros.isEmpty()) {
            return numItems;
        }
        // cumsum to get probability ranges
        double[] probs = Utils.cumSum(probList);
        double p;
        int id, i;
        while (estOracleSize < budget) {
            // select next cc
            p = rand.nextDouble();
            id = (Arrays.binarySearch(probs, p) + 1) * -1;
            i = invMap.get(id);
            if (numItems[i] == ccsSizes[i]) {
                zeros.set(i, false);
            } else if (numItems[i] < ccsSizes[i]) {
                numItems[i] += 1;
                estOracleSize += ccsSizes[i];
            }
            if (zeros.isEmpty()) {
                return numItems;
            }
        }
        System.out.println("estimated Oracle Size=" + estOracleSize);
        return numItems;
    }
    
}
