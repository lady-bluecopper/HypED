package eu.centai.hypeq.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import eu.centai.hyped.cc.CCSerializer;
import eu.centai.hyped.cc.ConnectedComponents;
import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.oracle.structures.OracleSerializer;
import eu.centai.hypeq.oracle.structures.SDistanceOracle;
import eu.centai.hypeq.oracle.structures.SOracleSerializer;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 *
 * @author giulia
 */
public class Writer {

    /**
     * Write statistics on disk.
     *
     * @param numQueries number of s-distance pairs computed
     * @param numL actual number of landmarks used
     * @param indexTime time required to construct the oracle
     * @param queryTime time required to compute the distance profiles
     * @param oracleSize size of the oracle
     * @throws IOException
     */
    public static void writeStats(int numQueries, int numL,
            long indexTime,
            long queryTime,
            int oracleSize) throws IOException {
        FileWriter fw = new FileWriter(Settings.outputFolder + "statistics.csv", true);
        String method = Settings.landmarkSelection;
        if (method.equalsIgnoreCase("bestcover") || method.equalsIgnoreCase("between")) {
            method += ("_" + Settings.samplePerc);
        }
        fw.write(String.format("%s\t%s\t%f\t%f\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%f\t%f\n",
                Settings.dataFile,
                new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()),
                indexTime / 1000.0D,
                queryTime / 1000.0D,
                Settings.maxS,
                Settings.lb,
                Settings.numLandmarks,
                numL,
                oracleSize,
                numQueries,
                method,
                Settings.landmarkAssignment,
                Settings.alpha,
                Settings.beta));
        fw.close();
    }
    
    /**
     * Write stats on s-line graph creation.
     * 
     * @param creationTime running time to create s-line graph
     * @param s min overlap size
     * @throws IOException 
     */
    public static void writeLGStats(long creationTime, int s) throws IOException {
        FileWriter fw = new FileWriter(Settings.outputFolder + "statistics.csv", true);
        fw.write(String.format("%s\t%s\t%f\t%d\n",
                Settings.dataFile,
                new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()),
                creationTime / 1000.0D,
                s));
        fw.close();
    }

    /**
     * Write landmarks selected by the oracle on disk.
     *
     * @param oracle distance oracle including the s-ccs
     * @param allLandmarks set of landmarks for each s
     * @param id identifier for the experiment
     * @throws IOException
     */
    public static void writeLandmarks(DistanceOracle oracle, List<Set<Integer>> allLandmarks,
            String id) throws IOException {

        String method = Settings.landmarkSelection;
        if (method.equalsIgnoreCase("bestcover") || method.equalsIgnoreCase("between")) {
            method += ("_" + Settings.samplePerc);
        }

        String fName = Settings.dataFile
                + "_LANDS"
                + "_S" + Settings.maxS
                + "_L" + Settings.numLandmarks
                + "_LB" + Settings.lb
                + "_M" + method
                + "_LA" + Settings.landmarkAssignment
                + "_A" + Settings.alpha
                + "_B" + Settings.beta
                + "_ID" + id + ".txt";
        FileWriter fwP = new FileWriter(Settings.outputFolder + fName);
        for (int i = 0; i < allLandmarks.size(); i++) {
            Set<Integer> lands = allLandmarks.get(i);
            int s = i + 1;
            for (int l : lands) {
                // landmark, s, id of s-cc, size of s-cc
                fwP.write(l + " " + s + " " + oracle.getIdOfSCC(l, s)
                        + " " + oracle.getSizeOf(s, oracle.getIdOfSCC(l, s)) + "\n");
            }
        }
        fwP.close();
    }

    /**
     * Write s-distance pairs on disk.
     *
     * @param real real distance profiles
     * @param approx approximate distance profiles
     * @param id identifier for the experiment
     * @throws IOException
     */
    public static void writeResults(Map<Pair<Integer, Integer>, DistanceProfile> real,
            Map<Pair<Integer, Integer>, DistanceProfile> approx,
            String id) throws IOException {

        String method = Settings.landmarkSelection;

        if (method.equalsIgnoreCase("bestcover") || method.equalsIgnoreCase("between")) {
            method += ("_" + Settings.samplePerc);
        }

        String fName = Settings.dataFile
                + "_S" + Settings.maxS
                + "_L" + Settings.numLandmarks
                + "_LB" + Settings.lb
                + "_Q" + Settings.numQueries
                + "_M" + method
                + "_LA" + Settings.landmarkAssignment
                + "_A" + Settings.alpha
                + "_B" + Settings.beta
                + "_ID" + id + ".txt";
        FileWriter fwP = new FileWriter(Settings.outputFolder + fName);
        if (real != null) {
            for (Pair<Integer, Integer> p : real.keySet()) {
                DistanceProfile realP = real.get(p);
                DistanceProfile approxP = approx.get(p);
                for (Entry<Integer, Triplet<Double, Double, Double>> entry : realP.getDistanceProfile().entrySet()) {
                    int s = entry.getKey();
                    // u v s real lb up approx
                    fwP.write(p.getValue0() + " " + p.getValue1() + " "
                            + s + " " + entry.getValue().getValue0() + " "
                            + approxP.getSLowBound(s) + " "
                            + approxP.getSUpBound(s) + " "
                            + approxP.getSDistance(s) + "\n");
                }
            }
        } else {
            for (Pair<Integer, Integer> p : approx.keySet()) {
                DistanceProfile approxP = approx.get(p);
                for (Entry<Integer, Triplet<Double, Double, Double>> entry : approxP.getDistanceProfile().entrySet()) {
                    fwP.write(p.getValue0() + " " 
                            + p.getValue1() + " "
                            + entry.getKey() + " " 
                            + entry.getValue().getValue2() + " "
                            + entry.getValue().getValue0() + " "
                            + entry.getValue().getValue1() + " "
                            + entry.getValue().getValue2() + "\n");
                }
            }
        }
        fwP.close();
    }
    
    /**
     * Write s-centrality values on disk.
     *
     * @param real real s-centralities
     * @param approx approximate s-centralities
     * @param id identifier for the experiment
     * @throws IOException
     */
    public static void writeCentralities(Map<Integer, Double> real,
            Map<Integer, Double> approx,
            String id) throws IOException {

        String method = Settings.landmarkSelection;

        if (method.equalsIgnoreCase("bestcover") || method.equalsIgnoreCase("between")) {
            method += ("_" + Settings.samplePerc);
        }

        String fName = Settings.dataFile
                + "_S" + Settings.maxS
                + "_L" + Settings.numLandmarks
                + "_LB" + Settings.lb
                + "_Q" + Settings.numQueries
                + "_M" + method
                + "_LA" + Settings.landmarkAssignment
                + "_A" + Settings.alpha
                + "_B" + Settings.beta
                + "_CENT" + id + ".txt";
        
        FileWriter fwP = new FileWriter(Settings.outputFolder + fName);
        approx.entrySet().forEach(en -> {
            double realC = real.getOrDefault(en.getKey(), en.getValue());
            try {
                fwP.write(en.getKey() + " " + realC + " " + en.getValue() + "\n");
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
        fwP.close();
    }
    
    /**
     * Write s-reachable elements on disk. 
     *
     * @param reachables each pair (u, lst) includes the list lst of elements 
     * s-reachable from u
     * @param id identifier for the experiment
     * @throws IOException
     */
    public static void writeReachable(List<Pair<Integer, List<Triplet<Integer, Integer, Double>>>> reachables,
            String id) throws IOException {

        String method = Settings.landmarkSelection;

        if (method.equalsIgnoreCase("bestcover") || method.equalsIgnoreCase("between")) {
            method += ("_" + Settings.samplePerc);
        }

        String fName = Settings.dataFile
                + "_S" + Settings.maxS
                + "_L" + Settings.numLandmarks
                + "_LB" + Settings.lb
                + "_Q" + Settings.numQueries
                + "_M" + method
                + "_LA" + Settings.landmarkAssignment
                + "_A" + Settings.alpha
                + "_B" + Settings.beta
                + "_ID" + id + ".txt";
        FileWriter fwP = new FileWriter(Settings.outputFolder + fName);
        reachables.stream().forEach(pair -> {
            for (Triplet<Integer, Integer, Double> t : pair.getValue1()) {
                try {
                    // source, destination, s, s-distance
                    fwP.write(pair.getValue0() + " "
                            + t.getValue1() + " "
                            + t.getValue0() + " "
                            + t.getValue2() + "\n");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        });
        fwP.close();
    }
    
    /**
     * Write top-k s-reachable elements, for various s and query elements, on disk. 
     *
     * @param reals for each s, for each element in queries, the sorted real top-k 
     * s-reachable elements; if Settings.isApproximate, then reals is empty.
     * @param approx for each s, for each element in queries, the sorted approximate
     * top-k s-reachable elements.
     * @param queries query elements
     * @param id identifier for the experiment
     * @throws IOException
     */
    public static void writeTopKReachable(Map<Integer, int[][]> reals,
            Map<Integer, int[][]> approx,
            List<Integer> queries,
            String id) throws IOException {

        String method = Settings.landmarkSelection;

        if (method.equalsIgnoreCase("bestcover") || method.equalsIgnoreCase("between")) {
            method += ("_" + Settings.samplePerc);
        }

        String fName = Settings.dataFile
                + "_S" + Settings.maxS
                + "_L" + Settings.numLandmarks
                + "_LB" + Settings.lb
                + "_Q" + Settings.numQueries
                + "_M" + method
                + "_LA" + Settings.landmarkAssignment
                + "_A" + Settings.alpha
                + "_B" + Settings.beta
                + "_K" + Settings.k
                + "_ID" + id + ".txt";
        FileWriter fwP = new FileWriter(Settings.outputFolder + fName);
        approx.entrySet().stream().forEach(entry -> {
            for (int i = 0; i < queries.size(); i++) {
                for (int j = 0; j < Settings.k; j++) {
                    try {
                        // query, neighbor (approx), neighbor (real), s
                        if (reals.isEmpty()) {
                            fwP.write(queries.get(i) + " "
                                    + entry.getValue()[i][j] + " "
                                    + entry.getValue()[i][j] + " "
                                    + entry.getKey() + "\n");
                        } else {
                            fwP.write(queries.get(i) + " "
                                    + entry.getValue()[i][j] + " "
                                    + reals.get(entry.getKey())[i][j] + " "
                                    + entry.getKey() + "\n");
                        }
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        });
        fwP.close();
    }
    
    /**
     * Write s-line graph on disk as list of edges (no weight).
     *
     * @param lineGraph line graph
     * @param s max overlap size
     * @throws IOException
     */
    public static void writeLineGraph(Map<Integer, Set<Pair<Integer, Integer>>> lineGraph, int s) throws IOException {

        String fName = Settings.dataFile.substring(0, Settings.dataFile.length() - 3)
                + "_S" + s + ".lg";
        FileWriter fwP = new FileWriter(Settings.outputFolder + fName);
        lineGraph.entrySet().forEach(entry -> 
                entry.getValue().stream().forEach(pair -> {
                    try {
                        fwP.write(entry.getKey() + " " + pair.getValue0() + "\n");
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                })
        );
        fwP.close();
    }
    
    /**
     * Write the distance oracle to disk.
     * 
     * @param oracle distance oracle
     * @throws FileNotFoundException 
     */
    public static void writeOracle(DistanceOracle oracle) throws FileNotFoundException {
        
        String method = Settings.landmarkSelection;

        if (method.equalsIgnoreCase("bestcover") || method.equalsIgnoreCase("between")) {
            method += ("_" + Settings.samplePerc);
        }
        
        String fName = Settings.dataFile + "_ORACLE"
                + "_S" + Settings.maxS
                + "_L" + Settings.numLandmarks
                + "_LB" + Settings.lb
                + "_M" + method
                + "_LA" + Settings.landmarkAssignment
                + "_A" + Settings.alpha
                + "_B" + Settings.beta;
        Output out = new Output(new FileOutputStream(Settings.outputFolder + fName));
        Kryo kryo = new Kryo();
        // register structures
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.Collection.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(int[].class);
        kryo.register(SDistanceOracle.class);
        kryo.register(java.util.HashSet.class);
        // initialize serializers for custom classes
        OracleSerializer ser2 = new OracleSerializer();
        kryo.register(DistanceOracle.class, ser2);
        SOracleSerializer ser3 = new SOracleSerializer();
        kryo.register(SDistanceOracle.class, ser3);
        // write
        kryo.writeObject(out, oracle);
        out.flush();
        out.close();
    }
    
    /**
     * Write the s-distance oracle to disk.
     * 
     * @param oracle s-distance oracle
     * @param s min overlap size
     * @throws FileNotFoundException 
     */
    public static void writeSOracle(SDistanceOracle oracle, int s) throws FileNotFoundException {
        
        String method = Settings.landmarkSelection;

        if (method.equalsIgnoreCase("bestcover") || method.equalsIgnoreCase("between")) {
            method += ("_" + Settings.samplePerc);
        }
        
        String fName = Settings.dataFile + "_SORACLE"
                + "_S" + s
                + "_L" + Settings.numLandmarks
                + "_LB" + Settings.lb
                + "_M" + method
                + "_LA" + Settings.landmarkAssignment
                + "_A" + Settings.alpha
                + "_B" + Settings.beta;
        Output out = new Output(new FileOutputStream(Settings.outputFolder + fName));
        Kryo kryo = new Kryo();
        // register structures
        kryo.register(java.util.HashMap.class);
        kryo.register(SDistanceOracle.class);
        kryo.register(java.util.HashSet.class);
        // initialize serializers for custom classes
        SOracleSerializer ser3 = new SOracleSerializer();
        kryo.register(SDistanceOracle.class, ser3);
        // write
        kryo.writeObject(out, oracle);
        out.flush();
        out.close();
    }
    
    /**
     * Write the connected components to disk.
     * 
     * @param CCS connected components up to maxS
     * @throws FileNotFoundException 
     */
    public static void writeConnectedComponents(ConnectedComponents CCS) throws FileNotFoundException {
        
        String fName = Settings.dataFile + "_CCS"
                + "_S" + Settings.maxS
                + "_LB" + Settings.lb;
        Output out = new Output(new FileOutputStream(Settings.outputFolder + fName));
        Kryo kryo = new Kryo();
        // register structures
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.Collection.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(java.util.HashSet.class);
        // initialize serializers for custom classes
        CCSerializer ser = new CCSerializer();
        kryo.register(ConnectedComponents.class, ser);
        PairSerializer ser2 = new PairSerializer();
        kryo.register(org.javatuples.Pair.class, ser2);
        // write
        kryo.writeObject(out, CCS);
        out.flush();
        out.close();
    }

}
