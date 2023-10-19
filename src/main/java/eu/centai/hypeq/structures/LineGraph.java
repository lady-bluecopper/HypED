package eu.centai.hypeq.structures;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;

public class LineGraph {
    
    private ObjectArrayList<LabeledNode> nodes;
    private Int2ObjectOpenHashMap<ObjectArrayList<int[]>> adj;
    private int numEdges;

    /**
     * Initialize a new line graph given a list of hyperedges.
     * 
     * @param hyperedges list of hyperedges
     */
    public LineGraph(List<HyperEdge> hyperedges) {
        this.nodes = new ObjectArrayList(hyperedges.size());
        this.adj = new Int2ObjectOpenHashMap(hyperedges.size());
        this.adj.defaultReturnValue(ObjectArrayList.of());
        this.numEdges = -1;
        initialize(hyperedges);
        System.out.println("LineGraph Initialized.");
    }
    
    public LineGraph(List<HyperEdge> hyperedges, FileWriter fw) throws IOException {
        findAndSave(hyperedges, fw);
        System.out.println("LineGraph Initialized and Saved.");
    }

    /**
     * Initialize a new line graph given a list of nodes and their connections.
     * 
     * @param nodes set of nodes
     * @param adj adjacency matrix
     */
    public LineGraph(ObjectArrayList<LabeledNode> nodes, 
            Int2ObjectOpenHashMap<ObjectArrayList<int[]>> adj) {
        this.nodes = nodes;
        this.adj = adj;
        this.numEdges = -1;
    }
    
    /**
     * 
     * @param nodes list of nodes that will become the vertex set of the line
     * graph
     */
    public void setNodes(ObjectArrayList<LabeledNode> nodes) {
        this.nodes = nodes;
        this.adj = new Int2ObjectOpenHashMap(nodes.size());
        this.adj.defaultReturnValue(ObjectArrayList.of());
    }
    
    /**
     * 
     * @return nodes in the line graph
     */
    public ObjectArrayList<LabeledNode> getNodes() {
        return nodes;
    }
    
    /**
     * 
     * @return number of nodes in the line graph
     */
    public int numberOfNodes() {
        return nodes.size();
    }
    
    /**
     * Exploits a dynamic inverted index to find hyperedges that overlap with a 
     * given hyperedge.

     * @param edge hyperedge
     * @param index inverted index
     * @return neighbours of edge found using the invertex index
     */
    private ObjectArrayList<LabeledEdge> findEdges(HyperEdge edge, 
            Int2ObjectOpenHashMap<IntArrayList> index) {
        Int2IntOpenHashMap ngb = new Int2IntOpenHashMap();
        ngb.defaultReturnValue(0);
        ObjectArrayList<LabeledEdge> edges = new ObjectArrayList();
        for (int v : edge.getVertices()) {
            for (int idx : index.getOrDefault(v, IntArrayList.of())) {
                if (edge.getId() != idx) {
                    ngb.addTo(idx, 1);
                }
            }
        }
        ngb.int2IntEntrySet()
                .stream()
                .forEach(e -> edges.add(new LabeledEdge(
                        edge.getId(),
                        e.getIntKey(),
                        e.getIntValue())));
        ngb = null;
        return edges;
    }
    
    /**
     * Construct a line graph from a hypergraph.
     * 
     * @param hyperedges list of hyperedges
     */
    private void initialize(List<HyperEdge> hyperedges) {

        long start = System.currentTimeMillis();
        ObjectBigArrayBigList<LabeledEdge> edges = new ObjectBigArrayBigList();
        // dynamic index
        Int2ObjectOpenHashMap<IntArrayList> index = new Int2ObjectOpenHashMap();
        index.defaultReturnValue(IntArrayList.of());
        int counter = 0;
        for (HyperEdge edge : hyperedges) {
            nodes.add(new LabeledNode(edge.getId(), edge.getNumVertices()));
            // find neighbours
            edges.addAll(findEdges(edge, index));
            // add edge to dynamic index
            for (int v: edge.getVertices()) {
                IntArrayList tmp = index.get(v);
                tmp.add(edge.getId());
                index.put(v, tmp);
            }
            counter += 1;
            if (counter % 10000 == 0) {
                System.out.println("counter=" + counter);
            }
        }
        System.out.println("Edge Initialization Completed in (ms) " + (System.currentTimeMillis() - start));
        initializeVMap(edges);
        edges = null;
        System.gc();
    }
    
    private void findAndSave(List<HyperEdge> hyperedges, FileWriter fw) throws IOException {
        // dynamic index
        Int2ObjectOpenHashMap<IntArrayList> index = new Int2ObjectOpenHashMap();
        index.defaultReturnValue(IntArrayList.of());
        ObjectArrayList<LabeledEdge> currEdges;
        int counter = 0;
        for (HyperEdge edge : hyperedges) {
            // find neighbours
            currEdges = findEdges(edge, index);
            // add edge to dynamic index
            for (int v: edge.getVertices()) {
                IntArrayList tmp = index.get(v);
                tmp.add(edge.getId());
                index.put(v, tmp);
            }
            counter += 1;
            if (counter % 10000 == 0) {
                System.out.println("counter=" + counter);
            }
            for (LabeledEdge edg : currEdges) {
                fw.write(edg.toSimpleString() + "\n");
            }
        }
        System.out.println("Edge Initialization Completed.");
    }
    
    /**
     * 
     * @param edges list of edges
     */
    private void initializeVMap(ObjectBigArrayBigList<LabeledEdge> edges) {
        long start = System.currentTimeMillis();
        ObjectArrayList<int[]> tmp;
        for (LabeledEdge e : edges) {
            tmp = adj.get(e.getSrc());
            tmp.add(new int[]{e.getDst(), e.getLabel()});
            adj.put(e.getSrc(), tmp);
            tmp = adj.get(e.getDst());
            tmp.add(new int[]{e.getSrc(), e.getLabel()});
            adj.put(e.getDst(), tmp);
        }
        System.out.println("VMap Initialization Completed in (ms) " + (System.currentTimeMillis() - start));
    }
    
    /**
     * 
     * @return number of edges in the line graph
     */
    public int getNumEdges() {
        if (this.numEdges == -1) {
            this.numEdges = adj.int2ObjectEntrySet().stream()
                    .flatMap(entry -> entry.getValue()
                            .stream().map(n -> (entry.getIntKey() < n[0]) ? 
                        new Pair<Integer, Integer>(entry.getIntKey(), n[0]) : 
                        new Pair<Integer, Integer>(n[0], entry.getIntKey())))
                    .collect(Collectors.toSet())
                    .size();
        }
        return this.numEdges;
    }
    
    /**
     * 
     * @param node id of a node
     * @return label of node
     */
    public int getNodeLabel(int node) {
        return nodes.get(node).getLabel();
    }
    
    /**
     * 
     * @param node id of a node
     * @return neighbours of node
     */
    public ObjectArrayList<int[]> getNeighbours(int node) {
        return adj.get(node);
    } 
    
    /**
     * 
     * @return adjacency matrix of the line graph
     */
    public Int2ObjectOpenHashMap<ObjectArrayList<int[]>> getAdjMap() {
        return adj;
    }
    
    /**
     * 
     * @return id of all the nodes in the line graph
     */
    public Set<Integer> getNodeIDs() {
        return nodes.stream().map(n -> n.getIndex()).collect(Collectors.toSet());
    }
    
    /**
     * Retains only the edges with weight not lower than s, and only the vertices 
     * corresponding to hyperedges of size not lower than s.
     * 
     * @param s weight
     * @return s-line graph
     */
    public LineGraph getProjection(int s) {
        List<LabeledNode> sNodes = getNodes()
                .stream()
                .filter(n -> n.getLabel() >= s)
                .collect(Collectors.toList());
        Int2ObjectOpenHashMap<ObjectArrayList<int[]>> sEdges = new Int2ObjectOpenHashMap(adj.size());
        sEdges.defaultReturnValue(ObjectArrayList.of());
        for (LabeledNode node : sNodes) {
            ObjectArrayList ngb = new ObjectArrayList(
                    getNeighbours(node.getIndex()).stream()
                            .filter(e -> e[1] >= s)
                            .collect(Collectors.toList()));
            sEdges.put(node.getIndex(), ngb);
        }
        return new LineGraph(new ObjectArrayList(sNodes), sEdges);
    }
    
}
