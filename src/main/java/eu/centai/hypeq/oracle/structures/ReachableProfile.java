package eu.centai.hypeq.oracle.structures;

import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 *
 * @author giulia
 */
public class ReachableProfile {
    
    private final int p;
    private String label;
    // (s, v, s-distance) for each element s-reachable from p
    private List<Triplet<Integer, Integer, Double>> reachables;
    
    /**
     * Initialize the ReachableProfile of an element (vertex or hyperedge).
     * 
     * @param p element for which the profile is created 
     */
    public ReachableProfile(int p) {
        this.p = p;
        this.reachables = Lists.newArrayList();
    }
    
    /**
     * Initialize the ReachableProfile of an element (vertex or hyperedge).
     * 
     * @param p element for which the profile is created 
     * @param label label associated to the element
     */
    public ReachableProfile(int p, String label) {
        this.p = p;
        this.label = label;
        this.reachables = Lists.newArrayList();
    }
    
    /**
     * @param distance s-distance to an element s-reachable from p
     */
    public void addReachable(Triplet<Integer, Integer, Double> distance) {
        this.reachables.add(distance);
    }
    
    /**
     * @param distances s-distances to elements s-reachable from p
     */
    public void addReachables(Collection<Triplet<Integer, Integer, Double>> distances) {
        this.reachables.addAll(distances);
    }
    
    /**
     * Add the s-distance from p to a reachable element *v*.
     * 
     * @param v element reachable from p
     * @param s min overlap size
     * @param d (approximate) s-distance from p to v
     */
    public void addReachable(int v, int s, double d) {
        reachables.add(new Triplet<>(s, v, d));
    }
    
    /** 
     * 
     * @param k parameter
     * @param s min-overlap size
     * @param labels if not null, it finds the top-k closest elements with the 
     * same label of p
     * @return k closest elements wrt s-distance (s, element, s-distance)
     */
    public List<Triplet<Integer, Integer, Double>> topKsreachable(int k, int s, 
            Map<Integer, String> labels) {
        // vertices currently selected
        PriorityQueue<Pair<Integer, Double>> selection = new PriorityQueue(
                new Comparator<Pair<Integer, Double>>(){
                    @Override
                    public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
                        return - Double.compare(o1.getValue1(), o2.getValue1());
                    }
                });
        System.out.println("Num Candidates: " + reachables.stream()
                .filter(t -> t.getValue0() == s && 
                        labels.getOrDefault(t.getValue1(), "NONE").equalsIgnoreCase(label))
                .count());
        reachables.stream()
                .filter(t -> t.getValue0() == s && 
                        labels.getOrDefault(t.getValue1(), "NONE").equalsIgnoreCase(label))
                .forEach(t -> {
                    if (selection.size() < k) {
                        selection.add(new Pair<>(t.getValue1(), t.getValue2()));
                    } else if (t.getValue2() < selection.peek().getValue1()) {
                        selection.poll();
                        selection.add(new Pair<>(t.getValue1(), t.getValue2()));
                    }
                });
        return selection
                .stream()
                .map(pair -> new Triplet<>(s, pair.getValue0(), pair.getValue1()))
                .collect(Collectors.toList());
    } 
    
    /**
     * Finds top-k reachable elements from p for each s.
     * 
     * @param k parameter
     * @param labels if not null, it finds the top-k closest elements with the 
     * same label of p
     * @return k closest elements wrt s-distance (s, element, s-distance), for each s
     */
    public List<Triplet<Integer, Integer, Double>> topKreachable(int k, 
            Map<Integer, String> labels) {
        int maxS = reachables.stream().mapToInt(t -> t.getValue0()).max().orElse(0);
        List<Triplet<Integer, Integer, Double>> output = Lists.newArrayList();
        for (int s = 1; s <= maxS; s++) {
            output.addAll(topKsreachable(k, s, labels));
        }
        return output;
    }
    
    /**
     * 
     * @return s-distances to the s-reachable elements
     */
    public List<Triplet<Integer, Integer, Double>> getReachables() {
        return reachables;
    }
    
    /**
     * 
     * @return element from which the s-distances are estimated/calculated 
     */
    public int getSource() {
        return p;
    }
    
    /**
     * 
     * @param s min overlap size
     * @return number of s-reachable elements
     */
    public long size(int s) {
        return reachables.stream()
                .filter(t -> t.getValue0() <= s)
                .mapToInt(t -> t.getValue1())
                .distinct()
                .count();
    }
}
