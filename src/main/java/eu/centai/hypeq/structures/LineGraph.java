package eu.centai.hypeq.structures;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;

public class LineGraph {
    
    private List<LabeledNode> nodes;
    private Map<Integer, Set<Pair<Integer, Integer>>> adj;
    
    /**
     * Initialize a new line graph given a list of hyperedges.
     * 
     * @param hyperedges list of hyperedges
     */
    public LineGraph(List<HyperEdge> hyperedges) {
        this.nodes = Lists.newArrayList();
        this.adj = Maps.newHashMap();
        initialize(hyperedges);
    }

    /**
     * Initialize a new line graph given a list of nodes and their connections.
     * 
     * @param nodes set of nodes
     * @param adj adjacency matrix
     */
    public LineGraph(List<LabeledNode> nodes, Map<Integer, Set<Pair<Integer, Integer>>> adj) {
        this.nodes = nodes;
        this.adj = adj;
    }
    
    /**
     * 
     * @param nodes list of nodes that will become the vertex set of the line
     * graph
     */
    public void setNodes(List<LabeledNode> nodes) {
        this.nodes = nodes;
        this.adj = Maps.newHashMap();
    }
    
    /**
     * 
     * @return nodes in the line graph
     */
    public List<LabeledNode> getNodes() {
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
    private List<LabeledEdge> findEdges(HyperEdge edge, Map<Integer, Set<Integer>> index) {
        Map<Integer, Integer> ngb = Maps.newHashMap();
        List<LabeledEdge> edges = Lists.newArrayList();
        for (int v : edge.getVertices()) {
            for (int idx : index.getOrDefault(v, Sets.newHashSet())) {
                if (edge.getId() != idx) {
                    ngb.put(idx, ngb.getOrDefault(idx, 0) + 1);
                }
            }
        }
        ngb.entrySet()
                .stream()
                .forEach(e -> edges.add(new LabeledEdge(edge.getId(), e.getKey(), e.getValue())));
        return edges;
    }
    
    /**
     * Construct a line graph from a hypergraph.
     * 
     * @param hyperedges list of hyperedges
     */
    private void initialize(List<HyperEdge> hyperedges) {
        List<LabeledEdge> edges = Lists.newArrayList();
        // dynamic index
        Map<Integer, Set<Integer>> index = Maps.newHashMap();
        
        for (HyperEdge edge : hyperedges) {
            nodes.add(new LabeledNode(edge.getId(), edge.getNumVertices()));
            // find neighbours
            edges.addAll(findEdges(edge, index));
            // add edge to dynamic index
            edge.getVertices().stream().forEach(v -> {
                Set<Integer> tmp = index.getOrDefault(v, Sets.newHashSet());
                tmp.add(edge.getId());
                index.put(v, tmp);
            });
        }
        initializeVMap(edges);
    }
    
    /**
     * 
     * @param edges list of edges
     */
    private void initializeVMap(List<LabeledEdge> edges) {
        for (LabeledEdge e : edges) {
            Set<Pair<Integer, Integer>> tmp = adj.getOrDefault(e.getSrc(), Sets.newHashSet());
            tmp.add(new Pair<>(e.getDst(), e.getLabel()));
            adj.put(e.getSrc(), tmp);
            Set<Pair<Integer, Integer>> tmp2 = adj.getOrDefault(e.getDst(), Sets.newHashSet());
            tmp2.add(new Pair<>(e.getSrc(), e.getLabel()));
            adj.put(e.getDst(), tmp2);
        }
    }
    
    /**
     * 
     * @return number of edges in the line graph
     */
    public int getNumEdges() {
        return adj.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(n -> 
            (entry.getKey() < n.getValue0()) ? 
                    new Pair<Integer, Integer>(entry.getKey(), n.getValue0()) : 
                    new Pair<Integer, Integer>(n.getValue0(), entry.getKey())))
                .collect(Collectors.toSet())
                .size();
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
    public Set<Pair<Integer, Integer>> getNeighbours(int node) {
        return adj.getOrDefault(node, Sets.newHashSet());
    } 
    
    /**
     * 
     * @return adjacency matrix of the line graph
     */
    public Map<Integer, Set<Pair<Integer, Integer>>> getAdjMap() {
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
        List<LabeledNode> sNodes = getNodes().stream().filter(n -> n.getLabel() >= s).collect(Collectors.toList());
        Map<Integer, Set<Pair<Integer, Integer>>> sEdges = Maps.newHashMap();
        sNodes.stream().map(node -> new Pair<Integer, Set<Pair<Integer, Integer>>>(node.getIndex(), 
                        getNeighbours(node.getIndex())
                                .stream()
                                .filter(e -> e.getValue1() >= s)
                                .collect(Collectors.toSet())))
                .forEach(p -> sEdges.put(p.getValue0(), p.getValue1()));
        return new LineGraph(sNodes, sEdges);
    }
    
}
