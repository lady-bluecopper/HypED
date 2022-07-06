package eu.centai.hypeq.structures;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author giulia
 */
public class HyperEdge {
    
    private final int id;
    private final Set<Integer> vertices;
    private final Map<Integer, Integer> neighbours;
    private final int size;
    
    public HyperEdge(int id, Set<Integer> vertices) {
        this.id = id;
        this.vertices = vertices;
        this.neighbours = Maps.newHashMap();
        this.size = vertices.size();
    }
    
    public Set<Integer> getVertices() {
        return vertices;
    }
    
    public int getNumVertices() {
        return size;
    }
    
    /**
     * 
     * @param v vertex id
     * @return true if this hyperedge contains vertex v
     */
    public boolean contains(int v) {
        return vertices.contains(v);
    }
    
    /**
     * 
     * @param vertices set of vertex ids
     * @return whether this hyperedge subsumes the hyperedge *vertices*
     */
    public boolean containsAll(Set<Integer> vertices) {
        return this.vertices.containsAll(vertices);
    }
    
    public int getId() {
        return id;
    }
    
    public Map<Integer, Integer> getNeighbourData() {
        return neighbours;
    }
    
    public Set<Integer> getNeighbours() {
        return neighbours.keySet();
    }
    
    /**
     * 
     * @param s min size
     * @return set of neighbors of this hyperedge with size not lower than s
     */
    public Set<Integer> getSNeighbours(int s) {
        return neighbours.entrySet().stream()
                .filter(e -> e.getValue() >= s)
                .map(e -> e.getKey())
                .collect(Collectors.toSet());
    }
    
    /**
     * 
     * @param s min overlap size
     * @param cands set of hyperedge ids
     * @return set of hyperedge ids of all the hyperedges in cands with 
     * overlap >= s with this hyperedge 
     */
    public Set<Integer> getRestrSNeighbours(int s, Set<Integer> cands) {
        return neighbours.entrySet()
                .stream()
                .filter(e -> e.getValue() >= s && cands.contains(e.getKey()))
                .map(e -> e.getKey())
                .collect(Collectors.toSet());
    }
    
    /**
     * 
     * @param s min size
     * @return number of neighbors of this hyperedge with size not lower than s
     */
    public int getNumSNeighbours(int s) {
        return (int) neighbours.entrySet().stream()
            .filter(e -> e.getValue() >= s)
            .count();
    }
    
    public void updateNeighbourData(Map<Integer, Integer> data) {
        neighbours.putAll(data);
    }
    
    /**
     * 
     * @param n hyperedge id
     * @param w s-distance from this hyperedge to n
     */
    public void addNeighbour(int n, int w) {
        neighbours.put(n, w);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HyperEdge that = (HyperEdge) o;
        return getVertices().equals(that.getVertices());
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 17 * hash + this.id;
        hash = 17 * hash + Objects.hashCode(this.vertices);
        return hash;
    }

    public HyperEdge copy() {
        Set<Integer> newV = Sets.newHashSet(getVertices());
        return new HyperEdge(getId(), newV);
    }
    
    @Override
    public String toString() {
        return getVertices().toString();
    }

}
