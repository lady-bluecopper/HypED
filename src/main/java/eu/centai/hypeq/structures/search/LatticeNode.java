package eu.centai.hypeq.structures.search;

import com.google.common.collect.Lists;
import java.util.List;

/**
 *
 * @author giulia
 */
public class LatticeNode {
    
    private final int id;
    private List<Integer> parents;
    
    public LatticeNode(int id, int parent) {
        // id of corresponding hyperedge
        this.id = id;
        // id parent node in the tree
        parents = Lists.newArrayList();
        parents.add(parent);
    }
    
    public List<Integer> getParents() {
        return parents;
    }
    
    public void addParent(int p) {
        parents.add(p);
    }
    
    public String toString() {
        return id + ": " + parents.toString();
    }
    
}
