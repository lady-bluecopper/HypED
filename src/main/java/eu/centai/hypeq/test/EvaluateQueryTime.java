package eu.centai.hypeq.test;

import com.google.common.collect.Lists;
import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.test.helpers.Helper;
import eu.centai.hypeq.utils.CMDLParser;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Utils;
import eu.centai.hypeq.utils.Writer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.javatuples.Pair;

/**
 * Class used to evaluate the performance and the accuracy of the oracle, when 
 * answering different kinds of queries (vertex to vertex, vertex to edge, edge 
 * to edge).
 * 
 * @author giulia
 */
public class EvaluateQueryTime {
    
    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);
        
        HyperGraph graph = null;
        Map<Integer, Set<Integer>> vMap;
        int maxD = Settings.maxS;
        // if we want exact distances, we need to load the graph as well
        if (Settings.isApproximate) {
            vMap = Reader.loadVMap(Settings.dataFolder + Settings.dataFile);
        } else {
            graph = Reader.loadGraph(Settings.dataFolder + Settings.dataFile, false);
            System.out.println("graph loaded.");
            maxD = Math.min(graph.getDimension(), maxD);
            vMap = graph.getVertexMap();
        }
        Pair<DistanceOracle, Long> p = Helper.getDistanceOracle(graph, maxD);
        DistanceOracle oracle = p.getValue0();
        // sample some hyperedges
        Set<Pair<Integer, Integer>> sample;
        // if a query file has been specified
        if (Settings.queryFile != null) {
            System.out.println("loading sample...");
            sample = Reader.readSample(Settings.dataFolder + Settings.queryFile);
        } else {
            System.out.println("extracting sample...");
            if (Settings.kind.equalsIgnoreCase("vertex")) {
                List<Integer> cands = Lists.newArrayList(vMap.keySet());
                Random rnd = new Random(Settings.seed);
                sample = Utils.samplePairs(cands, Settings.numQueries * 2, rnd);
            } else {
                sample = oracle.samplePairs(vMap, Settings.numQueries, Settings.seed, Settings.kind);
            }
        }
        // find real distances
        Map<Pair<Integer, Integer>, DistanceProfile> realDistances = null;
        if (!Settings.isApproximate) {
            System.out.println("finding real distances...");
            // find real distances
            realDistances = graph.computeRealDistanceProfilesAmong(sample, maxD, Settings.kind);
            System.out.println("found real distances for " + realDistances.size() + " pairs.");
        }
        // query time and approximations
        System.out.println("finding approx distances for " + sample.size() + " pairs...");
        StopWatch watch = new StopWatch();
        watch.start();
        Map<Pair<Integer, Integer>, DistanceProfile> approxDist = Helper.populateDistanceProfiles(vMap, oracle, sample);
        Writer.writeStats(sample.size(), oracle.getNumLandmarks(), p.getValue1(), watch.getElapsedTime(), oracle.getOracleSize());
        System.out.println("found distances for " + approxDist.size() + " pairs.");
        Writer.writeResults(realDistances, approxDist, Settings.kind + Settings.seed);
    }
    
}
