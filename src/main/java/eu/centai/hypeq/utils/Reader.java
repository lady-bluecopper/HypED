package eu.centai.hypeq.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.centai.hyped.cc.CCSerializer;
import eu.centai.hyped.cc.ConnectedComponents;
import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.oracle.structures.OracleSerializer;
import eu.centai.hypeq.oracle.structures.SDistanceOracle;
import eu.centai.hypeq.oracle.structures.SOracleSerializer;
import eu.centai.hypeq.structures.HyperEdge;
import eu.centai.hypeq.structures.HyperGraph;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 * Class with methods to read objects from disk.
 * @author giulia
 */
public class Reader {
    
    /**
     * Reads an unlabeled hypergraph.
     * 
     * @param fileName path of input file
     * @param computeNeighs whether the hyperedge overlaps should be computed at 
     * construction time
     * @return hypergraph from input file
     * @throws IOException 
     */
    public static HyperGraph loadGraph(String fileName, boolean computeNeighs) throws IOException {
        final BufferedReader rows = new BufferedReader(new FileReader(fileName));
        List<HyperEdge> edges = Lists.newArrayList();
        String line;
        int counter = 0;

        while ((line = rows.readLine()) != null) {
            String[] parts = line.split(" ");
            Set<Integer> tmp = Sets.newHashSet();
            for (String p : parts) {
                tmp.add(Integer.parseInt(p));
            }
            HyperEdge s = new HyperEdge(counter, tmp);
            edges.add(s);
            counter++;
        }
        rows.close();
        return new HyperGraph(edges, computeNeighs);
    }
    
    /**
     * @param fileName path of input file
     * @return for each vertex, set of hypeedges including that vertex
     * @throws IOException 
     */
    public static Map<Integer, Set<Integer>> loadVMap(String fileName) throws IOException {
        final BufferedReader rows = new BufferedReader(new FileReader(fileName));
        Map<Integer, Set<Integer>> vMap = Maps.newHashMap();
        String line;
        int counter = 0;

        while ((line = rows.readLine()) != null) {
            String[] parts = line.split(" ");
            for (String p : parts) {
                int v = Integer.parseInt(p);
                Set<Integer> memb = vMap.getOrDefault(v, Sets.newHashSet());
                memb.add(counter);
                vMap.put(v, memb);
            }
            counter++;
        }
        rows.close();
        return vMap;
    }
    
    /**
     * 
     * @param fileName path to input file
     * @return list of hyperedges of the hypergraph in the input file
     * @throws IOException 
     */
    public static List<HyperEdge> loadEdges(String fileName) throws IOException {
        final BufferedReader rows = new BufferedReader(new FileReader(fileName));
        List<HyperEdge> edges = Lists.newArrayList();
        String line;
        int counter = 0;

        while ((line = rows.readLine()) != null) {
            String[] parts = line.split(" ");
            Set<Integer> tmp = Sets.newHashSet();
            for (String p : parts) {
                tmp.add(Integer.parseInt(p));
            }
            HyperEdge s = new HyperEdge(counter, tmp);
            edges.add(s);
            counter++;
        }
        rows.close();
        return edges;
    }
    
    /**
     * 
     * @param fileName path to the label file
     * @return map of vertex/hyperedge id to its label
     * @throws IOException 
     */
    public static Map<Integer, String> loadLabels(String fileName) throws IOException {
        final BufferedReader rows = new BufferedReader(new FileReader(fileName));
        Map<Integer, String> labels = Maps.newHashMap();
        String line;

        while ((line = rows.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts[0].contains("-")) {
                String[] tmpId = parts[0].split("-");
                labels.put(Integer.parseInt(tmpId[tmpId.length-1]), parts[1]);
            } else {
                labels.put(Integer.parseInt(parts[0]), parts[1]);
            }
        }
        rows.close();
        return labels;
    }
    
    /**
     * 
     * @param fileName path of the input file
     * @return distance profiles of pairs of vertices/hyperedges
     * @throws IOException 
     */
    public static Map<Pair<Integer, Integer>, DistanceProfile> loadDistanceProfiles(String fileName) throws IOException {
        final BufferedReader rows = new BufferedReader(new FileReader(fileName));
        Map<Pair<Integer, Integer>, DistanceProfile> profiles = Maps.newHashMap();
        String line;
        
        while ((line = rows.readLine()) != null) {
            String[] parts = line.split(" ");
            int p = Integer.parseInt(parts[0]);
            int q = Integer.parseInt(parts[1]);
            Pair<Integer, Integer> pair = new Pair<>(p, q);
            DistanceProfile profile = profiles.getOrDefault(pair, new DistanceProfile(p,q));
            int s = Integer.parseInt(parts[2]);
            double lb = Double.parseDouble(parts[4]);
            double ub = Double.parseDouble(parts[5]);
            // we load only the real distance, as the approx can be obtained
            // from lb and ub
            double d = Double.parseDouble(parts[3]);
            profile.addDistance(s, lb, ub, d);
            profiles.put(pair, profile);
        }
        rows.close();
        return profiles;
    }
    
    /**
     * Read distance oracle from disk.
     * 
     * @return distance oracle loaded from the file
     * @throws FileNotFoundException 
     */
    public static DistanceOracle readOracle() throws FileNotFoundException {
        
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

        Input input = new Input(new FileInputStream(Settings.outputFolder + fName));
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
        // read from input
        DistanceOracle oracle = kryo.readObject(input, DistanceOracle.class);
        input.close();
        System.out.println("maxS=" + oracle.getNumSOracles() + 
                " O=" + oracle.getOracleSize() + 
                " L=" + oracle.getNumLandmarks());
        return oracle;
    }
    
    /**
     * Read s-distance oracle from disk.
     * 
     * @param s min overlap size
     * @return s-distance oracle loaded from the file
     * @throws FileNotFoundException 
     */
    public static SDistanceOracle readSOracle(int s) throws FileNotFoundException {
        
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

        Input input = new Input(new FileInputStream(Settings.outputFolder + fName));
        Kryo kryo = new Kryo();
        // register structures
        kryo.register(java.util.HashMap.class);
        kryo.register(SDistanceOracle.class);
        kryo.register(java.util.HashSet.class);
        // initialize serializers for custom classes
        SOracleSerializer ser3 = new SOracleSerializer();
        kryo.register(SDistanceOracle.class, ser3);
        // read from input
        SDistanceOracle oracle = kryo.readObject(input, SDistanceOracle.class);
        input.close();
        return oracle;
    }
    
    /**
     * Read connected components from disk.
     * 
     * @return connected components up to maxS
     * @throws FileNotFoundException 
     */
    public static ConnectedComponents readConnectedComponents() throws FileNotFoundException {
        
        String fName = Settings.dataFile + "_CCS"
                + "_S" + Settings.maxS
                + "_LB" + Settings.lb;

        Input input = new Input(new FileInputStream(Settings.outputFolder + fName));
        Kryo kryo = new Kryo();
        // register structures
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.Collection.class);
        kryo.register(java.util.ArrayList.class);
        // initialize serializers for custom classes
        CCSerializer ser = new CCSerializer();
        kryo.register(ConnectedComponents.class, ser);
        PairSerializer ser2 = new PairSerializer();
        kryo.register(org.javatuples.Pair.class, ser2);
        // read from input
        ConnectedComponents CCS = kryo.readObject(input, ConnectedComponents.class);
        input.close();
        return CCS;
    }
    
    /**
     * Reads pairs of elements from a file.
     * 
     * @param fileName path to the query file
     * @return a set of pairs of elements
     * @throws IOException 
     */
    public static Set<Pair<Integer, Integer>> readSample(String fileName) throws IOException {
        
        final BufferedReader rows = new BufferedReader(new FileReader(fileName));
        Set<Pair<Integer, Integer>> pairs = Sets.newHashSet();
        String line;

        while ((line = rows.readLine()) != null) {
            String[] parts = line.split(" ");
            Pair<Integer, Integer> p = new Pair<>(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
            pairs.add(p);
        }
        rows.close();
        return pairs;
    }
    
    /**
     * Reads query elements for from file.
     * 
     * @param fileName path of the query file
     * @return a set of elements (vertex or hyperedge ids)
     * @throws IOException 
     */
    public static List<Integer> readQueries(String fileName) throws IOException {
        
        final BufferedReader rows = new BufferedReader(new FileReader(fileName));
        List<Integer> queries = Lists.newArrayList();
        String line;
        while ((line = rows.readLine()) != null) {
            queries.add(Integer.parseInt(line.trim()));
        }
        rows.close();
        return queries;
    }
    
    /**
     * Reads pairs of elements from a file.
     * 
     * @param fileName path to the query file
     * @return a set of pairs of elements
     * @throws IOException 
     */
    public static Collection<Triplet<Integer, Integer, Integer>> readSQueries(String fileName) throws IOException {
        
        final BufferedReader rows = new BufferedReader(new FileReader(fileName));
        List<Triplet<Integer, Integer, Integer>> queries = Lists.newArrayList();
        String line;

        while ((line = rows.readLine()) != null) {
            String[] parts = line.split(" ");
            // source, destination, s
            Triplet<Integer, Integer, Integer> t = new Triplet<>(
                    Integer.parseInt(parts[0]), 
                    Integer.parseInt(parts[1]),
                    Integer.parseInt(parts[2]));
            queries.add(t);
        }
        rows.close();
        return queries;
    }
    
}
