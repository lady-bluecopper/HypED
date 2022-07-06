package eu.centai.hypeq.test;

import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.javatuples.Pair;
import org.javatuples.Triplet;

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
        
        HyperGraph graph = Reader.loadGraph(Settings.dataFolder + Settings.dataFile, false);
        int maxD = Math.min(graph.getDimension(), Settings.maxS);
        System.out.println("graph loaded.");
        Triplet<DistanceOracle, ConnectedComponents, Long> oracleP = Helper.getDistanceOracle(graph, maxD);
        DistanceOracle oracle = oracleP.getValue0();
        // sample some hyperedges
        System.out.println("extracting sample...");
        Set<Pair<Integer, Integer>> sample;
        if (Settings.kind.equalsIgnoreCase("vertex")) {
            List<Integer> cands = Lists.newArrayList(graph.getVertices());
            Random rnd = new Random(Settings.seed);
            sample = graph.samplePairs(cands, Settings.numQueries * 2, rnd);
        } else {
            sample = oracleP.getValue1().samplePairs(graph, Settings.numQueries, Settings.seed, Settings.kind);
        }
        // query time and approximations
        System.out.println("finding approx distances for " + sample.size() + " pairs...");
        StopWatch watch = new StopWatch();
        watch.start();
        Map<Pair<Integer, Integer>, DistanceProfile> approxDist = Helper.populateDistanceProfiles(graph, oracle, sample);
        Writer.writeStats(sample.size(), oracle.getNumLandmarks(), oracleP.getValue2(), watch.getElapsedTime(), oracle.getOracleSize());
        System.out.println("found distances for " + approxDist.size() + " pairs.");
        Map<Pair<Integer, Integer>, DistanceProfile> realDistances = null;
        if (!Settings.isApproximate) {
            System.out.println("finding real distances...");
            // find real distances
            realDistances = graph.computeRealDistanceProfilesAmong(sample, maxD, Settings.kind);
            System.out.println("found real distances for " + realDistances.size() + " pairs.");
        }
        Writer.writeResults(realDistances, approxDist, Settings.kind + Settings.seed);
    }
    
}
