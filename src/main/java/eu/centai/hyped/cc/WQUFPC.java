package eu.centai.hyped.cc;

import com.google.common.collect.Maps;
import eu.centai.hypeq.structures.LabeledNode;
import eu.centai.hypeq.structures.LineGraph;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;

/**
 *
 * @author giulia
 */
public class WQUFPC {

    /**
     * It supports the <em>union</em> and <em>find</em> operations, along with
     * methods for determining whether two sites are in the same component and
     * the total number of components.
     * <p>
     * This implementation uses quick union (by size/rank) with full path
     * compression. The constructor takes &Theta;(<em>n</em>) time, where
     * <em>n</em> is the number of elements. The <em>union</em> and
     * <em>find</em> operations take &Theta;(log <em>n</em>) time in the worst
     * case. The <em>count</em> operation takes &Theta;(1) time. Moreover,
     * starting from an empty data structure with <em>n</em> sites, any
     * intermixed sequence of <em>m</em> <em>union</em> and <em>find</em>
     * operations takes <em>O</em>(<em>m</em> &alpha;(<em>n</em>)) time, where
     * &alpha;(<em>n</em>) is the inverse of
     * <a href = "https://en.wikipedia.org/wiki/Ackermann_function#Inverse">Ackermann's
     * function</a>.
     * <p>
     *
     * @author Robert Sedgewick
     * @author Kevin Wayne
     */
    private int[] parent;  // parent[i] = parent of i
    private int[] rank;
    private int count;     // number of components

    /**
     * Initializes an empty union-find data structure with {@code n} elements
     * {@code 0} through {@code n-1}. Initially, each elements is in its own
     * set.
     *
     * @param n the number of elements
     * @throws IllegalArgumentException if {@code n < 0}
     */
    public WQUFPC(int n) {
        count = n;
        parent = new int[n];
        rank = new int[n];
        for (int i = 0; i < n; i++) {
            parent[i] = i;
            rank[i] = 0;
        }
    }

    /**
     * Returns the number of connected components.
     *
     * @return the number of connected components (between {@code 1} and {@code n})
     */
    public int count() {
        return count;
    }

    /**
     * Returns the root of the tree containing element {@code p}.
     *
     * @param p an element
     * @return the canonical element of the set containing {@code p}
     * @throws IllegalArgumentException unless {@code 0 <= p < n}
     */
    public int find(int p) {
        int root = p;
        // find root
        while (root != parent[root]) {
            root = parent[root];
        }
        // perform path compression
        while (p != root) {
            int newp = parent[p];
            parent[p] = root;
            p = newp;
        }
        return root;
    }
    
    /**
     * Interleaved Find: combines union and find.
     * Uses ranks to decide how to merge trees.
     * 
     * Originally from:
     * Manne and Patwary, 
     * A scalable parallel union-find algorithm for distributed memory computers,
     * PPAM, 2009.
     * 
     * @param p an element
     * @param q an element
     */
    public void eTvLSP(int p, int q) {
        int rootP = p;
        int rootQ = q;
        int z;
        
        while (parent[rootP] != parent[rootQ]) {
            if (rank[parent[rootP]] <= rank[parent[rootQ]]) {
                if (rootP == parent[rootP]) {
                    if (rank[parent[rootP]] == rank[parent[rootQ]]) {
                        rootQ = parent[rootQ];
                        if (rootQ == parent[rootQ]) {
                            rank[rootQ] += 1;
                        }
                    }
                    parent[rootP] = parent[rootQ];
                    count--;
                    break;
                }
                z = parent[rootP];
                parent[rootP] = parent[rootQ];
                rootP = z;
            } else {
                if (rootQ == parent[rootQ]) {
                    parent[rootQ] = parent[rootP];
                    count--;
                    break;
                }
                z = parent[rootQ];
                parent[rootQ] = parent[rootP];
                rootQ = z;
            }
        }
    }
    
    /**
     * Merges the set containing element {@code p} with the the set containing
     * element {@code q}.
     *
     * @param p one element
     * @param q the other element
     * @throws IllegalArgumentException unless both {@code 0 <= p < n} and
     * {@code 0 <= q < n}
     */
    public void union(int p, int q) {
        int rootP = find(p);
        int rootQ = find(q);
        if (rootP == rootQ) {
            return;
        }
        if (rank[rootP] < rank[rootQ]) {
            parent[rootP] = rootQ;
        }
        else if (rank[rootP] > rank[rootQ]) {
            parent[rootQ] = rootP;
        }
        else {
            parent[rootP] = rootQ;
            rank[rootQ] += 1;
        }
        count--;
    }
    
    /**
     * 
     * @param p hyperedge id
     * @param q hyperedge id
     * @return true if the two hyperedges have the same root
     */
    public boolean sameCC(int p, int q) {
                
        return find(p) == find(q);
    }
    
    /**
     * Exploits a known connected component cc to initialize the parent id of the 
     * elements in cc.
     * 
     * @param cc connected component
     */
    public void initializeFromCC(List<Integer> cc) {
        rank[cc.get(0)] = cc.size();
        cc.stream().forEach(i -> parent[i] = cc.get(0));
    }
    
    /**
     * Finds the connected components in the hypergraph, whose neighboring 
     * hyperedges are given as input.
     *
     * @param overlaps list of pairs of hyperedges that overlap
     */
    public void findCCsHyperEdges(List<Pair<Integer, Integer>> overlaps) {
        for (Pair<Integer, Integer> ov : overlaps) {
            int p = ov.getValue0();
            int q = ov.getValue1();
            if (find(p) == find(q)) {
                continue;
            }
//            eTvLSP(p, q);
            union(p, q);
        }
    }
    
    /**
     * Finds the connected components in the line graph.
     *
     * @param graph LineGraph
     */
    public void findCCsLineGraph(LineGraph graph) {
        Map<Integer, Integer> vMap = Maps.newHashMap();
        int counter = 0;
        for (LabeledNode v : graph.getNodes()) {
            vMap.put(v.getIndex(), counter);
            counter ++;
        }
        graph.getAdjMap().int2ObjectEntrySet().forEach(e -> {
            int p = vMap.get(e.getIntKey());
            for (int[] pair : e.getValue()) {
                int q = vMap.get(pair[0]);
                if (p < q) {
                    if (find(p) == find(q)) {
                        continue;
                    }
//                    eTvLSP(p, q);
                    union(p,q);
                }
            }
        });
        System.out.println(count + " components");
    }
    
}
