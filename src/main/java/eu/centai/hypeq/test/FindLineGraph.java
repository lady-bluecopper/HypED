package eu.centai.hypeq.test;

import eu.centai.hyped.cc.WQUFPC;
import eu.centai.hypeq.structures.HyperEdge;
import eu.centai.hypeq.structures.LineGraph;
import eu.centai.hypeq.utils.CMDLParser;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Writer;
import java.util.List;

/**
 * Class used to find the s-line graphs of the hypergraph.
 * 
 * @author giulia
 */
public class FindLineGraph {
    
    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);
        
        List<HyperEdge> edges = Reader.loadEdges(Settings.dataFolder + Settings.dataFile);
        StopWatch watch = new StopWatch();
        watch.start();
        LineGraph lg = new LineGraph(edges);
        long creationT = watch.getElapsedTime();
        System.out.println("L_V=" + lg.numberOfNodes() + " L_E=" + lg.getNumEdges());
        for (int s = 1; s <= Settings.maxS; s++) {
            watch.start();
            LineGraph sg = lg.getProjection(s);
            if (!sg.getNodes().isEmpty()) {
                WQUFPC uf = new WQUFPC(sg.numberOfNodes());
                uf.findCCsLineGraph(sg);
                Writer.writeLGStats(creationT + watch.getElapsedTime(), s);
                Writer.writeLineGraph(sg.getAdjMap(), s);
            }
        }
    }
}
