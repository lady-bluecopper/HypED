package eu.centai.hypeq.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.centai.hyped.cc.WQUFPC;
import eu.centai.hypeq.structures.HyperEdge;
import eu.centai.hypeq.structures.HyperGraph;
import eu.centai.hypeq.structures.LineGraph;
import eu.centai.hypeq.utils.CMDLParser;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;

/**
 *  Class for comparing the strategies to find the connected components of a 
 * hypergraph, for all s.
 * 
 * @author giulia
 */
public class CompareCCStrategies {
    
    // find s-connected components using line graph
    private static void useLineGraph(List<HyperEdge> edges, int maxS) {
        LineGraph lg = new LineGraph(edges);
        for (int s = 1; s <= maxS; s++) {
            LineGraph sg = lg.getProjection(s);
            WQUFPC uf = new WQUFPC(sg.numberOfNodes());
            uf.findCCsLineGraph(sg);
        }
    }
    
    // find s-connected components using hyperedges overlaps
    private static void useHyperEdgeOverlaps(List<HyperEdge> hedges, int maxS) {
        for (int s1 = 1; s1 <= maxS; s1++) {
            final int s = s1;
            // inverted index v -> list of hyperedges including v
            Map<Integer, List<Integer>> vIndex = Maps.newHashMap();
            hedges.stream()
                    .filter(e -> e.getNumVertices() >= s)
                    .forEach(e -> e.getVertices()
                        .stream()
                        .forEach(v -> {
                            List<Integer> tmp = vIndex.getOrDefault(v, Lists.newArrayList());
                            tmp.add(e.getId());
                            vIndex.put(v, tmp);
                        })
                    );
            // sorted to process each pair of hyperedges by id
            vIndex.values().forEach(lst -> Collections.sort(lst));
            // adjacency matrix for hyperedges
            Map<Integer, Map<Integer, Integer>> eIndex = Maps.newHashMap();
            for (int v : vIndex.keySet()) {
                for (int i = 0; i < vIndex.get(v).size(); i++) {
                    // first hyperedge
                    int e1 = vIndex.get(v).get(i);
                    for (int j = i + 1; j < vIndex.get(v).size(); j++) {
                        // second hyperedge
                        int e2 = vIndex.get(v).get(j);
                        // update overlap
                        Map<Integer, Integer> tmpI = eIndex.getOrDefault(e1, Maps.newHashMap());
                        tmpI.put(e2, tmpI.getOrDefault(e2, 0)+1);
                        eIndex.put(e1, tmpI);
                    }
                }
            }
            // use s-overlaps to run union-find algorithm 
            List<Pair<Integer, Integer>> edges = eIndex.entrySet()
                    .parallelStream()
                    .flatMap(entry -> entry.getValue().entrySet()
                            .stream()
                            // overlaps >= s
                            .filter(e -> e.getValue() >= s)
                            .map(e -> new Pair<Integer, Integer>(entry.getKey(), e.getKey())))
                    .collect(Collectors.toList());
            WQUFPC uf = new WQUFPC(hedges.size());
            uf.findCCsHyperEdges(edges);
            Set<Integer> ccs = Sets.newHashSet();
            hedges.stream()
                    .filter(e -> e.getNumVertices() >= s)
                    .forEach(e -> ccs.add(uf.find(e.getId())));
        }
    }
    
    private static void useUnionFind(HyperGraph graph, int maxS) {
        graph.simplifiedConnectedComponents(maxS);
    }
    
    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);
        
        StopWatch watch = new StopWatch();
        HyperGraph graph = Reader.loadGraph(Settings.dataFolder + Settings.dataFile, false);
        List<HyperEdge> edges = Reader.loadEdges(Settings.dataFolder + Settings.dataFile);
        
        int maxD = Math.min(graph.getDimension(), Settings.maxS);
        
        watch.start();
        useLineGraph(edges, maxD);
        double time1 = watch.getElapsedTimeInSec();
        watch.start();
        useHyperEdgeOverlaps(edges, maxD);
        double time2 = watch.getElapsedTimeInSec();
        watch.start();
        useUnionFind(graph, maxD);
        double time3 = watch.getElapsedTimeInSec();
        System.out.println(Settings.dataFile + "\t" + maxD
                + "\t" + time1  + "\t" + time2 + "\t" + time3);
    }
    
}
