package eu.centai.hypeq.structures.search;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author giulia
 */
public class Lattice {
    
    private Map<Integer, LatticeNode> nodes;
    BloomFilter filter;
    
    public Lattice(int estSize) {
        this.nodes = Maps.newHashMap();
        this.filter = BloomFilter.create(Funnels.integerFunnel(), estSize, 0.01);
    }
    
    public void addNode(int id, int parent) {
        if (nodes.containsKey(id)) {
            nodes.get(id).addParent(parent);
        } else {
            LatticeNode node = new LatticeNode(id, parent);
            nodes.put(id, node);
            filter.put(id);
        }
    }
    
    public List<Set<Integer>> getPathsToId(int id) {
        if (!filter.mightContain(id) || !nodes.containsKey(id)) {
            return Lists.newArrayList();
        }
        return stepBack(Sets.newHashSet(), id);
    }
    
    private List<Set<Integer>> stepBack(Set<Integer> partialPath, int id) {
        List<Set<Integer>> paths = Lists.newArrayList();
        if (id == -1) {
            paths.add(partialPath);
            return paths;
        }
        partialPath.add(id);
        List<Integer> ps = nodes.get(id).getParents();
        for (int p : ps) {
            Set<Integer> pathCopy = Sets.newHashSet(partialPath);
            paths.addAll(stepBack(pathCopy, p));
        }
        return paths;
    }
    
    public Set<Integer> getNodes() {
        return nodes.keySet();
    }
    
}
