package eu.centai.hypeq.test;

import eu.centai.hyped.cc.WQUFPC;
import eu.centai.hypeq.structures.HyperEdge;
import eu.centai.hypeq.structures.LineGraph;
import eu.centai.hypeq.utils.CMDLParser;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Writer;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Class used to find the s-line graphs of the hypergraph.
 * 
 * @author giulia
 */
public class FindLineGraph {
    
//    public static void main(String[] args) throws Exception {
//        //parse the command line arguments
//        CMDLParser.parse(args);
//        String fName = Settings.dataFile.substring(0, Settings.dataFile.length() - 3) + ".lg";
//        FileWriter fwP = new FileWriter(Settings.outputFolder + fName);
//        
//        List<HyperEdge> edges = Reader.loadEdges(Settings.dataFolder + Settings.dataFile);
//        System.out.println("Dataset read from disk.");
//
//        LineGraph lg = new LineGraph(edges, fwP);
//        fwP.close();
//    }
    
    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);
        
        List<HyperEdge> edges = Reader.loadEdges(Settings.dataFolder + Settings.dataFile);
        System.out.println("Dataset read from disk.");
        StopWatch watch = new StopWatch();
        watch.start();
        LineGraph lg = new LineGraph(edges);
        long creationT = watch.getElapsedTime();
        for (int s = 1; s <= Settings.maxS; s++) {
            System.out.println("Examining s=" + s);
            watch.start();
            LineGraph sg = lg.getProjection(s);
            if (!sg.getNodes().isEmpty()) {
//                WQUFPC uf = new WQUFPC(sg.numberOfNodes());
//                uf.findCCsLineGraph(sg);
                Writer.writeLGStats(creationT + watch.getElapsedTime(), s);
                Writer.writeLineGraph(sg.getAdjMap(), s);
            }
        }
    }
}
