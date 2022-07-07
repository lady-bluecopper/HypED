package eu.centai.hypeq.test;

import eu.centai.hyped.cc.ConnectedComponents;
import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.test.helpers.Helper;
import eu.centai.hypeq.utils.CMDLParser;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Writer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 * Class used to compare the performance of three kinds of oracles.
 * 
 * @author giulia
 */
public class CompareWithBaseline {
    
    /**
     * 
     * @param graph hypergraph
     * @return distance oracles created using 3 different strategies and the creation times
     */
    public static Pair<DistanceOracle[], Long[]> compareCreationTimes(HyperGraph graph) 
            throws FileNotFoundException {
        
        double[] importance;
        if (Settings.landmarkAssignment.equals("ranking")) {
            importance = new double[]{Settings.alpha, Settings.alpha, Settings.beta, 1.};
        } else {
            importance = new double[]{Settings.alpha, Settings.beta};
        }
        Long[] creationTimes = new Long[3];
        DistanceOracle[] oracles = new DistanceOracle[3];
        int maxD = Math.min(graph.getDimension(), Settings.maxS);
        StopWatch watch = new StopWatch();
        watch.start();
        DistanceOracle oracle1 = new DistanceOracle();
        oracle1.populateOracleBaseline(
                graph, Settings.landmarkSelection, maxD, 
                Settings.numLandmarks, Settings.seed);
        creationTimes[0] = watch.getElapsedTime();
        oracles[0] = oracle1;
        System.out.println("Baseline 1 created.");
        watch.start();
        DistanceOracle oracle2 = new DistanceOracle();
        oracle2.populateOraclesCCIS(
                graph, Settings.landmarkSelection, maxD, 
                Settings.numLandmarks, Settings.lb, 
                importance, Settings.seed);
        creationTimes[1] = watch.getElapsedTime();
        oracles[1] = oracle2;
        System.out.println("Baseline 2 created.");
        watch.start();
        DistanceOracle oracle3 = new DistanceOracle();
        oracle3.populateOracles(
                graph, Settings.landmarkSelection, 
                Settings.landmarkAssignment, maxD, 
                Settings.numLandmarks, Settings.lb, 
                importance, true, Settings.seed);
        creationTimes[2] = watch.getElapsedTime();
        oracles[2] = oracle3;
        System.out.println("Baseline 3 created.");
        return new Pair<>(oracles, creationTimes);
    }
    
    /**
     * 
     * @param graph hypergraph
     * @param oracles distance oracles
     * @param realDistances real distance profiles
     * @param id sample id
     * @return query time of each distance oracle
     * @throws IOException 
     */
    public static Long[] compareQueryTimes(
            HyperGraph graph,
            DistanceOracle[] oracles, 
            Map<Pair<Integer, Integer>, DistanceProfile> realDistances,
            int id) throws IOException {
        
        Long[] times = new Long[3];
        Settings.kind = "edge";
        Map<Pair<Integer, Integer>, DistanceProfile> approxDist;
        StopWatch watch = new StopWatch();
        watch.start();
        approxDist = Helper.populateDistanceProfiles(graph.getVertexMap(), oracles[0], realDistances.keySet());
        times[0] = watch.getElapsedTime();
        Writer.writeResults(realDistances, approxDist, "bas1_sample" + id);
        watch.start();
        approxDist = Helper.populateDistanceProfiles(graph.getVertexMap(), oracles[1], realDistances.keySet());
        times[1] = watch.getElapsedTime();
        Writer.writeResults(realDistances, approxDist, "bas2_sample" + id);
        watch.start();
        approxDist = Helper.populateDistanceProfiles(graph.getVertexMap(), oracles[2], realDistances.keySet());
        times[2] = watch.getElapsedTime();
        Writer.writeResults(realDistances, approxDist, "meth_sample" + id);
        return times;
    }
    
    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);
        
        HyperGraph graph = Reader.loadGraph(Settings.dataFolder + Settings.dataFile, true);
        int maxD = Math.min(graph.getDimension(), Settings.maxS);
        System.out.println("Graph loaded.");
        // creation times
        Pair<DistanceOracle[], Long[]> oracles = compareCreationTimes(graph);
        System.out.println("oracles created.");
        // store landmarks
        String[] ids = new String[]{"bas1", "bas2", "meth"};
        for (int j = 0; j < 3; j++) {
            DistanceOracle oracle = oracles.getValue0()[j];
            Writer.writeLandmarks(oracles.getValue0()[2], oracle.getAllLandmarks(), ids[j]);
        }
        for (int i = 0; i < 5; i++) {
            System.out.println("Test " + i);
            // sample some hyperedges
            Set<Pair<Integer, Integer>> sample = oracles.getValue0()[2].samplePairs(graph.getVertexMap(), Settings.numQueries, i, "edge");
            // find real distances
            Map<Pair<Integer, Integer>, DistanceProfile> realDistances = graph.computeRealEdgeDistanceProfilesAmong(sample, maxD);
            // query time and approximations
            Long[] queryTimes = compareQueryTimes(graph, oracles.getValue0(), realDistances, i);
            for (int j = 0; j < 3; j++) {
                DistanceOracle oracle = oracles.getValue0()[j];
                Writer.writeStats(realDistances.size(), oracle.getNumLandmarks(), 
                        oracles.getValue1()[j], queryTimes[j], oracle.getOracleSize());
            }
        }
    }
}
