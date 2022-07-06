package eu.centai.hypeq.structures.search;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author giulia
 */
public class Tree {
    
    private Map<Integer, TreeNode> nodes;
    BloomFilter filter;
    
    public Tree(int estSize) {
        this.nodes = Maps.newHashMap();
        this.filter = BloomFilter.create(Funnels.integerFunnel(),
                estSize, 
                0.01);
    }
    
    /**
     * Add node to the tree
     * @param id node id to add
     * @param parent parent id of the node to add
     */
    public void addNode(int id, int parent) {
        TreeNode node = new TreeNode(id, parent);
        nodes.put(id, node);
        filter.put(id);
    }
    
    /**
     * 
     * @param id destination node
     * @return path from the tree source to id
     */
    public Set<Integer> getPathToId(int id) {
        Set<Integer> path = Sets.newHashSet();
        path.add(id);
        int p = nodes.get(id).getParent();
        while (p != -1) {
            path.add(p);
            p = nodes.get(p).getParent();
        }
        return path;
    }
    
    /**
     * 
     * @param v node id to check for membership
     * @param exact whether the answer should be exact or approximate
     * @return false if the tree does not contain v; true if the tree (might)
     * contain v
     */
    public boolean hasBeenVisited(int v, boolean exact) {
        if (exact) {
           return nodes.containsKey(v); 
        }
        return mightContain(v);
    }
    
    public Set<Integer> getNodes() {
        return nodes.keySet();
    }
    
    /**
     * 
     * @param t Tree
     * @return a path from the root of this tree to the root of t, if it exists
     */
    public Set<Integer> treeIntersect(Tree t) {
        List<Integer> cands = nodes.keySet()
                .stream()
                .filter(k -> t.mightContain(k))
                .collect(Collectors.toList());
        if (cands.isEmpty()) {
            return Sets.newHashSet();
        }
        return cands.stream()
                .filter(k -> t.hasBeenVisited(k, true))
                .map(k -> {
                    Set<Integer> first = getPathToId(k);
                    first.addAll(t.getPathToId(k));
                    return first;
                })
                .min((Set<Integer> o1, Set<Integer> o2) -> Integer.compare(o1.size(), o2.size()))
                .orElse(Sets.newHashSet());
    }
    
    /**
     * 
     * @param element node id to check for membership
     * @return if true, the tree might include element; if false, the tree does not 
     * contain element
     */
    public boolean mightContain(int element) {
        return filter.mightContain(element);
    }
}
