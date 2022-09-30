package eu.centai.hypeq.test;

import com.google.common.collect.Maps;
import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.oracle.structures.ReachableProfile;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.test.helpers.Helper;
import eu.centai.hypeq.utils.CMDLParser;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Writer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 * Finds the top-k reachable elements from a query element, using the oracle.
 * 
 * @author giulia
 */
public class TopKReachable {
    
    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);
        // load graph
        HyperGraph graph = null;
        Map<Integer, Set<Integer>> vMap;
        int maxD = Settings.maxS;
        // if we want exact distances, we need to load the graph as well
        if (Settings.isApproximate) {
            vMap = Reader.loadVMap(Settings.dataFolder + Settings.dataFile);
        } else {
            graph = Reader.loadGraph(Settings.dataFolder + Settings.dataFile, true);
            System.out.println("graph loaded.");
            maxD = Math.min(graph.getDimension(), maxD);
            vMap = graph.getVertexMap();
        }
        Pair<DistanceOracle, Long> p = Helper.getDistanceOracle(graph, maxD);
        DistanceOracle oracle = p.getValue0();
        System.out.println("loading queries...");
        List<Integer> queries = Reader.readQueries(Settings.dataFolder + Settings.queryFile);
        StopWatch watch = new StopWatch();
        watch.start();
        Map<Integer, int[][]> approx = Helper.findATopKReachable(vMap, 
                oracle, 
                queries, 
                maxD, 
                Settings.k,
                Settings.kind);
        long runtime = watch.getElapsedTime();
        Map<Integer, int[][]> reals = Maps.newHashMap();
        if (!Settings.isApproximate) {
            System.out.println("finding real reachables...");
            reals = graph.findTopKReachable(queries, maxD, Settings.k, Settings.kind);
        }
        Writer.writeStats(queries.size(), oracle.getNumLandmarks(), p.getValue1(), runtime, oracle.getOracleSize());
        Writer.writeTopKReachable(reals, approx, queries, "Kreach" + Settings.seed);
    }
    
}
