package eu.centai.hypeq.utils;

/**
 *
 * @author giulia
 */
public class Settings {
    
    public static String dataFolder = "";
    public static String outputFolder = "";
    public static String dataFile = "";
    public static String queryFile = null;
    // parameter used to determine the desired oracle size: numLandmarks * |E|
    public static int numLandmarks = 30;
    // (random, degree, farthest, bestcover, between)
    public static String landmarkSelection = "degree";
    // percentage of hyperedge to sample for finding paths (bestcover, between)
    public static double samplePerc = 0.4;
    // vertices/hyperedges to sample for querying
    public static int numQueries = 100;
    // (prob, ranking)
    public static String landmarkAssignment = "prob";
    // lower-bound to the size of the s-ccs to consider for landmark assignment
    public static int lb = 4;
    // max overlap size s
    public static int maxS = 10;
    // importance of size (prob: alpha + beta <= 1; ranking: alpha <= 1)
    public static double alpha = 0.2;
    // importance of s (prob: alpha + beta <= 1; ranking: beta <= 1)
    public static double beta = 0.6;
    // seed for reproducibility
    public static int seed = 4;
    // whether the oracle should be stored on disk
    public static boolean store = false;
    // whether we want to find only the approx distances or also the exact ones
    public static boolean isApproximate = true;
    // kind of distance to compute, among 
    // 1. "edge" (edge to edge)
    // 2. "vertex" (vertex to vertex)
    // 3. "both" (vertex to edge)
    public static String kind = "edge";
}
