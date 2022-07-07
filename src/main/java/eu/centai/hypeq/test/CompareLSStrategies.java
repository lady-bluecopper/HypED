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
 * Class used to compare the performance of the oracle, varying landmark selection
 * strategy.
 * 
 * @author giulia
 */
public class CompareLSStrategies {

    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);

        HyperGraph graph = Reader.loadGraph(Settings.dataFolder + Settings.dataFile, false);
        // settings
        String[] strategies = new String[]{"random", "degree", "farthest", "bestcover", "between"};
        double[] importance;
        if (Settings.landmarkAssignment.equals("ranking")) {
            importance = new double[]{Settings.alpha, Settings.alpha, Settings.beta, 1.};
        } else {
            importance = new double[]{Settings.alpha, Settings.beta};
        }
        for (String strat : strategies) {
            Settings.landmarkSelection = strat;
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
