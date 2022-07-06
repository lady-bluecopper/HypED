package eu.centai.hypeq.structures.search;

/**
 *
 * @author giulia
 */
public class TreeNode {
    
    private final int id;
    private final int parent;
    
    public TreeNode(int id, int parent) {
        // id of corresponding hyperedge
        this.id = id;
        // id parent node in the tree
        this.parent = parent;
    }
    
    public int getParent() {
        return parent;
    }
    
    public String toString() {
        return String.valueOf(id);
    }
    
}
