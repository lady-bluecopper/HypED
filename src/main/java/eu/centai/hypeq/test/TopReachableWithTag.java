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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 * (i) Selects 5 labels at random, (ii) samples elements with that labels, (iii) 
 * performs a BFS from them, stopping as soon as it reaches 20 hyperedges with 
 * the same label as the query, and (iv) finds the approximate distances 
 * using the oracle.
 * 
 * @author giulia
 */
public class TopReachableWithTag {
    
    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);
        // load graph
        HyperGraph graph = Reader.loadGraph(Settings.dataFolder + Settings.dataFile, false);
        int maxD = Math.min(graph.getDimension(), Settings.maxS);
        System.out.println("graph loaded with " + graph.getNumEdges() + " hyperedges.");
        // sample query elements
        String labelName = Settings.dataFile.substring(0, Settings.dataFile.length() - 3) + "_labels";
        String labelPath = Settings.dataFolder + labelName;
        // load labels
        Map<Integer, String> labels = Reader.loadLabels(labelPath);
        System.out.println("sampling labels...");
        List<List<Integer>> sampleList = Helper.sampleByTag(labels, 5);
        List<Integer> sample = sampleList.stream().flatMap(l -> l.stream()).collect(Collectors.toList());
        // create oracle
        Pair<DistanceOracle, Long> oracleP = Helper.getDistanceOracle(graph, maxD);
        DistanceOracle oracle = oracleP.getValue0();
        System.out.println("finding reachable hyperedges...");
        Map<Pair<Integer, Integer>, DistanceProfile> reals = 
                graph.findTopReachableFromWTag(sample, maxD, labels, 20, Settings.kind);
        // query time and approximations
        System.out.println("finding aprroximate distances...");
        StopWatch watch = new StopWatch();
        watch.start();
        Map<Pair<Integer, Integer>, DistanceProfile> approx = Helper.populateDistanceProfiles(graph.getVertexMap(), oracle, reals.keySet());
        Writer.writeStats(reals.size(), oracle.getNumLandmarks(), oracleP.getValue1(), watch.getElapsedTime(), oracle.getOracleSize());
        Writer.writeResults(reals, approx, "reachWTag" + Settings.seed);
    }
    
}
