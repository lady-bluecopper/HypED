package eu.centai.hypeq.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
public class Utils {

    /**
     * 
     * @param s1 set of integers
     * @param s2 set of integers
     * @return true if the two sets have a common value; false otherwise
     */
    public static boolean intersect(Set<Integer> s1, Set<Integer> s2) {
        for (int e : s1) {
            if (s2.contains(e)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 
     * @param s1 set of integers 
     * @param s2 set of integers
     * @return common values in the two sets
     */
    public static Set<Integer> intersection(Set<Integer> s1, Set<Integer> s2) {
        Set<Integer> intersection = Sets.newHashSet(s1);
        intersection.retainAll(s2);
        return intersection;
    }

    /**
     * 
     * @param s1 set of integers
     * @param s2 set of integers
     * @return number of common values in the two sets
     */
    public static int intersectionSize(Set<Integer> s1, Set<Integer> s2) {
        return (int) s1.stream().filter(e -> s2.contains(e)).count();
    }
    
    /**
     * 
     * @param s1 set of integers
     * @param s2 set of integers
     * @param maxI maximum number of common values considered
     * @return number of common values in the two sets, stopping at maxI
     */
    public static int cappedIntersectionSize(Set<Integer> s1, Set<Integer> s2, int maxI) {
        int count = 0;
        for (int e : s1) {
            if (s2.contains(e)) {
                count++;
                if (count >= maxI) {
                    return count;
                }
            }
        }
        return count;
    }

    /**
     * 
     * @param numbers array of integers
     * @return cumulative sum of numbers
     */
    public static int[] cumSum(int[] numbers) {
        int sum = 0;
        for (int i = 0; i < numbers.length; i++) {
            sum += numbers[i];
            numbers[i] = sum;
        }
        return numbers;
    }
    
    /**
     * 
     * @param numbers array of doubles
     * @return cumulative sum of numbers
     */
    public static double[] cumSum(List<Double> numbers) {
        double[] cumsum = new double[numbers.size()];
        double sum = 0.;
        for (int i = 0; i < numbers.size(); i++) {
            sum += numbers.get(i);
            cumsum[i] = sum;
        }
        return cumsum;
    }
    
    /**
     * 
     * @param numbers array of Double
     * @return cumulative sum of numbers
     */
    public static Double[] cumSum(Double[] numbers) {
        double sum = 0.;
        for (int i = 0; i < numbers.length; i++) {
            sum += numbers[i];
            numbers[i] = sum;
        }
        return numbers;
    }
    
    /**
     * 
     * @param array array of integers
     * @return position of the max integer in the array
     */
    public static int argMax(int[] array) {
        int max = Integer.MIN_VALUE;
        int id = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i] > max) {
                max = array[i];
                id = i;
            }
        }
        return id;
    }
    
    /**
     * 
     * @param array array of doubles
     * @return position of the max value in the array
     */
    public static int argMax(double[] array) {
        double max = Double.NEGATIVE_INFINITY;
        int id = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i] > max) {
                max = array[i];
                id = i;
            }
        }
        return id;
    }
    
    /**
     * 
     * @param array array of integers
     * @return position of the min value in the array
     */
    public static int argMin(int[] array) {
        int min = Integer.MAX_VALUE;
        int id = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i] < min) {
                min = array[i];
                id = i;
            }
        }
        return id;
    }
    
    /**
     * 
     * @param array array of doubles
     * @return position of the min value in the array
     */
    public static int argMin(double[] array) {
        double min = Double.POSITIVE_INFINITY;
        int id = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i] < min) {
                min = array[i];
                id = i;
            }
        }
        return id;
    }
    
    /**
     * 
     * @param a array of integers
     * @param b array of integers
     * @return dot product between the two arrays
     */
    public static int vDot(int[] a, int[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("The two vectors must have the same length!");
        }
        int sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += (a[i] * b[i]);
        }
        return sum;
    }
    
    /**
     * 
     * @param a array of doubles
     * @param b array of doubles
     * @return dot product between the two arrays
     */
    public static int vDot(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("The two vectors must have the same length!");
        }
        int sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += (a[i] * b[i]);
        }
        return sum;
    }
    
    /**
     * 
     * @param a matrix of integers
     * @param b array of integers
     * @return dot product
     */
    public static int[] dot(int[][] a, int[] b) {
        if (a[0].length != b.length) {
            throw new IllegalArgumentException("The two arguments must have the same number of columns!");
        }
        int[] sum = new int[a.length];
        for (int i = 0; i < a.length; i++) {
            for (int j = 0; j < a[i].length; j++) {
                sum[i] += (a[i][j] * b[j]);
            }
        }
        return sum;
    }
    
    /**
     * 
     * @param a matrix of integers
     * @param b double
     * @return dot product
     */
    public static double[][] dot(int[][] a, double b) {
        double[][] M = new double[a.length][a[0].length];
        for (int i = 0; i < a.length; i++) {
            for (int j = 0; j < a[i].length; j++) {
                M[i][j] = a[i][j] * b;
            }
        }
        return M;
    }
    
    /**
     * 
     * @param a array of integers
     * @param b array of integers
     * @return array of absolute differences between a and b
     */
    public static int[] abs(int[] a, int[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("The two arrays must have the same length!");
        }
        int[] c = new int[a.length];
        for (int i = 0; i < a.length; i++) {
            c[i] = Math.abs(a[i] - b[i]);
        }
        return c;
    }
    
    /**
     * 
     * @param a array of integers
     * @return sum of the array
     */
    public static int sum(int[] a) {
        int sum = 0;
        for (int x : a) {
            sum += x;
        }
        return sum;
    }
    
    /**
     * Sort the entries of a map by value.
     * 
     * @param c map entries
     * @param reverse whether the order should be descending or not
     */
    public static void sortMapByValue(List<Entry<Integer, Double>> c, boolean reverse) {
        Collections.sort(c, (Entry<Integer, Double> o1, Entry<Integer, Double> o2) -> {
            if (reverse) {
                return - Double.compare(o1.getValue(), o2.getValue());
            }
            return Double.compare(o1.getValue(), o2.getValue());
        });
    } 
    
    /**
     * Sort the entries of a map by value.
     * 
     * @param c map
     * @param reverse whether the order should be descending or not
     * @return sorted list of entries
     */
    public static List<Entry<Integer, Integer>> sortMapByValue(Map<Integer, Integer> c, boolean reverse) {
        List<Entry<Integer, Integer>> tmp = Lists.newArrayList(c.entrySet());
        Collections.sort(tmp, (Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) -> {
            if (reverse) {
                return - Integer.compare(o1.getValue(), o2.getValue());
            }
            return Integer.compare(o1.getValue(), o2.getValue());
        });
        return tmp;
    }
    
    /**
     * 
     * @param data array of doubles
     * @return geometric mean
     */
    public static double geometricMean(double[] data) {
	double sum = data[0];
	for (int i = 1; i < data.length; i++) {
		sum *= data[i]; 
	}
	return Math.pow(sum, 1.0 / data.length); 
    }
    
    /**
     * 
     * @param data array of doubles
     * @return arithmetic mean
     */
    public static double mean(double[] data) {
	double sum = 0;
	for (int i = 0; i < data.length; i++) {
		sum += data[i]; 
	}
        return sum / data.length;
    }
    
    /**
     * 
     * @param data array of doubles
     * @return median of the array
     */
    public static double median(double[] data) {
        Arrays.sort(data);
        if (data.length % 2 == 0) {
            return (data[data.length / 2] + data[data.length / 2 - 1]) / 2;
        }
        return data[(int) Math.ceil(data.length / 2)];
    }
    
    /**
     * @param candidates ids of candidates
     * @param sampleSize number of pairs to sample
     * @param rnd for reproducibility
     * @return a sample of pairs of candidates
     */
    public static Set<Pair<Integer, Integer>> samplePairs(List<Integer> candidates, 
            int sampleSize, 
            Random rnd) {
        // each triplet: ids of the items selected and size of the cc
        Set<Pair<Integer, Integer>> sample = Sets.newHashSet();
        // create pairs using candidate items
        List<Integer> items = selectItems(sampleSize, candidates, rnd);
        for (int i = 0; i < items.size() - 1; i += 2) {
            sample.add(new Pair<>(items.get(i), items.get(i + 1)));
        }
        if (items.size() % 2 != 0) {
            sample.add(new Pair<>(items.get(0), items.get(items.size() - 1)));
        }
        return sample;
    }
    
    /**
     * Method used to sample random elements from a list of candidates.
     *
     * @param numItems number of elements to select
     * @param candidates ids of candidate elements
     * @param rnd for reproducibility
     * @return elements selected from candidates
     */
    private static List<Integer> selectItems(int numItems, 
            List<Integer> candidates, 
            Random rnd) {
        // if candidates does not include at least 2 hyperedges
        // return empty
        if (candidates.size() < 2 || numItems == 0) {
            return Lists.newArrayList();
        }
        if (candidates.size() == numItems) {
            return Lists.newArrayList(candidates);
        }
        // initialize structures
        Set<Integer> items = Sets.newHashSet();
        while (items.size() < numItems) {
            items.add(candidates.get(rnd.nextInt(candidates.size())));
        }
        return Lists.newArrayList(items);
    }
    
    /**
     * Tries to add *budget* pairs of elements to the sample.
     * Some pairs may already been present in the sample, due to previous
     * iterations.
     * 
     * @param lists list of lists (e.g., list of s-connected components)
     * @param sample pairs sampled in previous iterations
     * @param budget number of pairs to sample
     * @param rnd random object
     */
    public static void sampleInLists(
            List<List<Integer>> lists, 
            Set<Pair<Integer, Integer>> sample,
            int budget, 
            Random rnd) {
        
        List<Integer> selectable = IntStream.range(0, lists.size())
                .filter(i -> lists.get(i).size() > 2)
                .boxed()
                .collect(Collectors.toList());
        
        BigInteger numCandPairs = BigInteger.ZERO;
        BigInteger one = BigInteger.ONE;
        BigInteger two = one.add(one);
        BigInteger B = BigInteger.valueOf(budget - sample.size());
        for (int i : selectable) {
            BigInteger size = BigInteger.valueOf(lists.get(i).size());
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
                Iterator<int[]> it = CombinatoricsUtils.combinationsIterator(lists.get(i).size(), 2);
                while (it.hasNext()) {
                    int[] comb = it.next();
                    int fst = lists.get(i).get(comb[0]);
                    int sec = lists.get(i).get(comb[1]);
                    sample.add(fst < sec
                            ? new Pair<>(fst, sec) 
                            : new Pair<>(sec, fst));
                }
            });
            return;
        }
        // otherwise we select them at random
        while (sample.size() < budget) {
            List<Integer> selectedCC = lists.get(selectable.get(rnd.nextInt(selectable.size())));
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
