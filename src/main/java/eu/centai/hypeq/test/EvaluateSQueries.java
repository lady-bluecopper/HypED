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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 * Class used to evaluate the performance and the accuracy of the oracle, when 
 * answering s-queries for a given value of s specified by the user in a query file. 
 * 
 * @author giulia
 */
public class EvaluateSQueries {
    
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
        System.out.println("loading queries...");
        Collection<Triplet<Integer, Integer, Integer>> queries = Reader.readSQueries(Settings.dataFolder + Settings.queryFile);
        // find real distances
        Map<Pair<Integer, Integer>, DistanceProfile> realDistances = null;
        if (!Settings.isApproximate) {
            System.out.println("finding real distances...");
            // find real distances
            realDistances = graph.computeRealDistanceProfilesAmong(queries, Settings.kind);
            System.out.println("found real distances.");
        }
        // query time and approximations
        System.out.println("finding approx distances...");
        StopWatch watch = new StopWatch();
        watch.start();
        Map<Pair<Integer, Integer>, DistanceProfile> approxDist = Helper.answerSDistanceQueries(vMap, oracle, queries);
        Writer.writeStats(queries.size(), oracle.getNumLandmarks(), p.getValue1(), watch.getElapsedTime(), oracle.getOracleSize());
        System.out.println("found approx distances.");
        Writer.writeResults(realDistances, approxDist, Settings.kind + Settings.seed);
    }
    
}
