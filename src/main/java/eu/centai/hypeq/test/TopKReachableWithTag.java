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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 * (i) Selects 5 labels at random, (ii) samples elements with that labels, (iii) 
 * performs a BFS from them, stopping as soon as it reaches k hyperedges with 
 * the same label as the query, and (iv) finds the top-k reachable elements 
 * using the oracle.
 * 
 * @author giulia
 */
public class TopKReachableWithTag {
    
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
        // sample query elements
        String labelName = Settings.dataFile.substring(0, Settings.dataFile.length() - 3) + "_labels";
        String labelPath = Settings.dataFolder + labelName;
        // load labels
        Map<Integer, String> labels = Reader.loadLabels(labelPath);
        List<Integer> sample;
        if (Settings.queryFile != null) {
            sample = Reader.readQueries(Settings.dataFolder + Settings.queryFile);
        } else {
            System.out.println("sampling labels...");
            List<List<Integer>> sampleList = Helper.sampleByTag(labels, 5);
            sample = sampleList.stream().flatMap(l -> l.stream()).collect(Collectors.toList());
        }
        System.out.println("finding reachables from " + sample.size() + " elements...");
        StopWatch watch = new StopWatch();
        watch.start();
        List<ReachableProfile> profiles = sample.stream()
                .parallel()
                .map(e -> oracle.createReachableProfile(vMap, e, labels.get(e), Settings.kind))
                .collect(Collectors.toList());
        System.out.println("extracting top-k reachables...");
        List<Pair<Integer, List<Triplet<Integer, Integer, Double>>>> topk = profiles
                .stream()
                .parallel()
                .map(rp -> new Pair<>(rp.getSource(), rp.topKreachable(Settings.k, labels)))
                .collect(Collectors.toList());
        long runtime = watch.getElapsedTime();
        Map<Pair<Integer, Integer>, DistanceProfile> reals = Maps.newHashMap();
        if (!Settings.isApproximate) {
            System.out.println("finding real reachables...");
            reals = graph.findTopReachableFromWTag(sample, maxD, labels, Settings.k, Settings.kind);
        }
        Writer.writeStats(profiles.size(), oracle.getNumLandmarks(), p.getValue1(), runtime, oracle.getOracleSize());
        Writer.writeReachable(topk, "reachWTag" + Settings.seed);
        Writer.writeResults(null, reals, "reachWTagEX");
    }
    
}
