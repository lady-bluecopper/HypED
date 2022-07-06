package eu.centai.hypeq.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
    
}
