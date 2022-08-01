package eu.centai.hypeq.test;

import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.test.helpers.Helper;
import eu.centai.hypeq.utils.CMDLParser;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Writer;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;

/**
 * Class for comparing the performance of the oracle, varying the importance factors
 * used in the landmark assignment process.
 * 
 * @author giulia
 */
public class CompareImportanceF {

    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);

        HyperGraph graph = Reader.loadGraph(Settings.dataFolder + Settings.dataFile, true);
        // settings
        double[] alphas;
        double[] betas;
        if (Settings.landmarkAssignment.equals("ranking")) {
            alphas = new double[]{0.5, 0.8, 1.0};
            betas = new double[]{0.5, 0.8, 1.0};
        } else {
            alphas = new double[]{0.2, 0.33, 0.4};
            betas = new double[]{0};
        }
        for (double alpha : alphas) {
            Settings.alpha = alpha;
            for (double beta : betas) {
                double[] importance;
                if (Settings.landmarkAssignment.equals("ranking")) {
                    Settings.beta = beta;
                    importance = new double[]{Settings.alpha, Settings.alpha, Settings.beta, 1.};
                } else {
                    Settings.beta = 1 - alpha * 2;
                    importance = new double[]{Settings.alpha, Settings.beta};
                }
                StopWatch watch = new StopWatch();
                watch.start();
                // create oracle
                DistanceOracle oracle = new DistanceOracle();
                oracle.populateOracles(
                        graph, Settings.landmarkSelection,
                        Settings.landmarkAssignment, Settings.maxS,
                        Settings.numLandmarks, Settings.lb, 
                        importance, false, Settings.seed);
                long creationTime = watch.getElapsedTime();
                System.out.println("oracle created.");
                // sample some hyperedges
                for (int i = 0; i < 5; i++) {
                    System.out.println("Test " + i);
                    Set<Pair<Integer, Integer>> sample = oracle.samplePairs(graph.getVertexMap(), Settings.numQueries, i, "edge");
                    // find real distances
                    Map<Pair<Integer, Integer>, DistanceProfile> realDistances = graph.computeRealEdgeDistanceProfilesAmong(sample, Settings.maxS);
                    // query time and approximations
                    watch.start();
                    Settings.kind = "edge";
                    Map<Pair<Integer, Integer>, DistanceProfile> approxDist = Helper.populateDistanceProfiles(graph.getVertexMap(), oracle, realDistances.keySet());
                    Writer.writeStats(realDistances.size(), oracle.getNumLandmarks(), creationTime, watch.getElapsedTime(), oracle.getOracleSize());
                    Writer.writeResults(realDistances, approxDist, "sample" + i);
                }
            }
        }

    }

}
