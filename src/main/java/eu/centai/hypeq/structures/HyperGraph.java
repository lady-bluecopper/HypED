package eu.centai.hypeq.structures;

import eu.centai.hypeq.structures.search.Lattice;
import eu.centai.hypeq.structures.search.Tree;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.centai.hyped.cc.ConnectedComponents;
import eu.centai.hyped.cc.WQUFPC;
import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.utils.Utils;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 *
 * @author giulia
 */
public class HyperGraph {

    private final List<HyperEdge> hyperedges;
    // for each vertex v, set of hyperedges including v
    private final Map<Integer, Set<Integer>> vertexMap;
    private int dimension;

    public HyperGraph(List<HyperEdge> edges, boolean initializeOverlaps) {
        this.hyperedges = edges;
        this.vertexMap = Maps.newHashMap();
        initializeVertexMap();
        if (initializeOverlaps) {
            initializeNeighbours();
        }
    }

    private void initializeVertexMap() {
        hyperedges.stream().forEach(edge -> {
            this.dimension = Math.max(dimension, edge.getNumVertices());
            for (int v : edge.getVertices()) {
                Set<Integer> memb = this.vertexMap.getOrDefault(v, Sets.newHashSet());
                memb.add(edge.getId());
                this.vertexMap.put(v, memb);
            }
        });
        System.out.println("V=" + vertexMap.size() + 
                ", E=" + hyperedges.size() + 
                ", d=" + dimension);
    }

    private void initializeNeighbours() {
        Map<Integer, List<Integer>> vIndex = Maps.newHashMap();

        hyperedges.stream().forEach(edge -> {
            Map<Integer, Integer> curr = findNeighbours(edge, vIndex);
            // update neighbour info
            edge.updateNeighbourData(curr);
            curr.entrySet().stream().forEach(e -> {
                hyperedges.get(e.getKey()).addNeighbour(edge.getId(), e.getValue());
            });
            // update dynamic index
            for (int v : edge.getVertices()) {
                List<Integer> tmpList = vIndex.getOrDefault(v, Lists.newArrayList());
                tmpList.add(edge.getId());
                vIndex.put(v, tmpList);
            }
        });
    }

    /**
     * When the hyperedge overlaps have not been computed at creation time, this
     * method exploits the candidate neighbors discovered when searching for the
     * s-connected components, to find the actual neighbors of the hyperedges.
     *
     * @param CCS connected components
     * @param maxS max overlap size
     * @param lb we do not need to compute the neighbors of hyperedges in s-cc
     * with size not greater than lb
     */
    public void initializeNeighboursFromCandidates(ConnectedComponents CCS, int maxS, int lb) {
        // first consider all partial overlaps 
        CCS.getOverlaps().entrySet().stream()
                .forEach(p -> {
                    Pair<Integer, Integer> pair = p.getKey();
                    hyperedges.get(pair.getValue0()).addNeighbour(pair.getValue1(), p.getValue());
                    hyperedges.get(pair.getValue1()).addNeighbour(pair.getValue0(), p.getValue());
                });
        // for each hyperedge, consider the list of candidate neighbours
        Map<Integer, List<Integer>> candsUnion = CCS.getCandsNeigh()
                .parallelStream()
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.groupingBy(e -> e.getKey(),
                        Collectors.mapping(e -> e.getValue(), Collectors.reducing(Lists.newArrayList(),
                                (List<Integer> t, List<Integer> u) -> {
                                    Set<Integer> union = Sets.newHashSet(t);
                                    union.addAll(u);
                                    return Lists.newArrayList(union);
                                })
                        )
                ));
        candsUnion.entrySet()
                .stream()
                .filter(e -> CCS.getSizeOf(1, CCS.getIdOfSCC(e.getKey(), 1)) > lb)
                .forEach(e -> {
                    Set<Integer> candidates = Sets.newHashSet(e.getValue());
                    // remove those we already know are neighbours
                    candidates.removeAll(getNeighborsOf(e.getKey()));
                    Map<Integer, Integer> curr = findNeighboursAmongCands(e.getKey(), candidates, maxS);
                    // update neighbour info
                    getEdge(e.getKey()).updateNeighbourData(curr);
                    curr.entrySet().stream().forEach(n -> {
                        getEdge(n.getKey()).addNeighbour(e.getKey(), n.getValue());
                    });
                });
    }

    /**
     *
     * @param edge hyperedge
     * @param vI vertex map
     * @return map with entries (hyperedge id, overlap size), for all the
     * hyperedges adjacent to edge
     */
    private Map<Integer, Integer> findNeighbours(HyperEdge edge, Map<Integer, List<Integer>> vI) {
        Map<Integer, Integer> curr = Maps.newHashMap();
        for (int v : edge.getVertices()) {
            for (int n : vI.getOrDefault(v, Lists.newArrayList())) {
                if (n != edge.getId()) {
                    curr.put(n, curr.getOrDefault(n, 0) + 1);
                }
            }
        }
        return curr;
    }

    /**
     *
     * @param edge hyperedge
     * @param cands candidate neighbors of edge
     * @param maxS max overlap size to consider
     * @return map with entries (hyperedge id, approx overlap size), for all the
     * hyperedges in cands adjacent to edge
     */
    private Map<Integer, Integer> findNeighboursAmongCands(int edge, Collection<Integer> cands, int maxS) {
        return cands.parallelStream()
                .map(cand -> new Pair<Integer, Integer>(cand, Utils.cappedIntersectionSize(getEdge(edge).getVertices(),
                getEdge(cand).getVertices(), maxS)))
                .collect(Collectors.toMap(x -> x.getValue0(), x -> x.getValue1()));
    }

    /**
     *
     * @param e hyperedge id
     * @return ids of all the hyperedges adjacent to e
     */
    public Set<Integer> getNeighborsOf(int e) {
        return hyperedges.get(e).getNeighbours();
    }

    /**
     *
     * @param e hyperedge id
     * @param s min overlap size
     * @return ids of all the hyperedges s-adjacent to e
     */
    public Set<Integer> getSNeighborsOf(int e, int s) {
        return hyperedges.get(e).getSNeighbours(s);
    }

    /**
     *
     * @param e hyperedge id
     * @param s min overlap size
     * @return number of hyperedges s-adjacent to e
     */
    public int getNumSNeighborsOf(int e, int s) {
        return hyperedges.get(e).getNumSNeighbours(s);
    }

    /**
     *
     * @param s min overlap size
     * @return number of hyperedges s-adjacent to each hyperedge
     */
    public Map<Integer, Integer> getNumSNeighborsOfAll(int s) {
        Map<Integer, Integer> neigh = Maps.newHashMap();
        hyperedges.stream().forEach(e -> neigh.put(e.getId(), e.getNumSNeighbours(s)));
        return neigh;
    }

    /**
     *
     * @return number of distinct vertices in the hypergraph
     */
    public int getNumVertices() {
        return vertexMap.size();
    }
    
    /**
     * 
     * @return for each vertex, the set of hyperedges including that vertex
     */
    public Map<Integer, Set<Integer>> getVertexMap() {
        return vertexMap;
    }
    
    /**
     * 
     * @return set of vertices in this hypergraph
     */
    public Set<Integer> getVertices() {
        return vertexMap.keySet();
    }

    /**
     *
     * @param e hyperedge id
     * @return vertices in e
     */
    public Set<Integer> getVerticesOf(int e) {
        return hyperedges.get(e).getVertices();
    }

    /**
     *
     * @param e hyperedge id
     * @return number of vertices in e
     */
    public int getNumVerticesOf(int e) {
        return hyperedges.get(e).getNumVertices();
    }

    /**
     *
     * @param edge hyperedge id
     * @param s min overlap size
     * @return s-degree of hyperedge edge
     */
    public int getEdgeDegree(int edge, int s) {
        return hyperedges.get(edge).getNumSNeighbours(s);
    }

    /**
     *
     * @param v vertex
     * @param s min size
     * @return ids of hyperedges of size >= s containing vertex *v*
     */
    public Set<Integer> getSHyperEdgesOf(int v, int s) {
        return vertexMap.get(v).stream()
                .filter(e -> hyperedges.get(e).getNumVertices() >= s)
                .collect(Collectors.toSet());
    }
    
    /**
     * 
     * @param v vertex
     * @return number of hyperedges containing v
     */
    public int getNumHyperEdgesOf(int v) {
        return vertexMap.get(v).size();
    }

    /**
     *
     * @return hyperedges in the hypergraph
     */
    public List<HyperEdge> getEdges() {
        return hyperedges;
    }

    /**
     *
     * @param s min size
     * @return hyperedges with size not lower than s
     */
    public List<Integer> getEdgesWithMinSize(int s) {
        if (s == 1) {
            return hyperedges.stream()
                    .map(e -> e.getId())
                    .collect(Collectors.toList());
        }
        return hyperedges.parallelStream()
                .filter(e -> e.getNumVertices() >= s)
                .map(e -> e.getId())
                .collect(Collectors.toList());
    }

    /**
     *
     * @param id hyperedge id
     * @return hyperedge with id *id*
     */
    public HyperEdge getEdge(int id) {
        return hyperedges.get(id);
    }

    /**
     *
     * @return number of hyperedges in the hypergraph
     */
    public int getNumEdges() {
        return hyperedges.size();
    }

    /**
     *
     * @param s min hyperedge size
     * @return number of hyperedges with size not lower than s
     */
    public long getNumEdgesWithMinSize(int s) {
        return hyperedges.stream().filter(e -> e.getNumVertices() >= s).count();
    }

    /**
     *
     * @return size of the largest hyperedge in the hypergraph
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * Find all the s-connected components for each s up to maxS, at once.
     * 
     * @param maxS max s searched
     * @return the s-connected components for each s not greater than maxS
     */
    public ConnectedComponents findConnectedComponents(int maxS) {
        // create structures
        ConnectedComponents CCS = new ConnectedComponents();
        Map<Integer, List<Integer>> vIndex = Maps.newHashMap();
        // instantiate BitSet to keep track of processed edges
        BitSet mask = new BitSet(getNumEdges());
        // start from largest s, to exploit neighbouring info
        for (int s = maxS; s > 0; s--) {
            List<Integer> sEdgeView = getEdgesWithMinSize(s);
            Map<Integer, Integer> invMap = Maps.newHashMap();
            for (int i = 0; i < sEdgeView.size(); i++) {
                invMap.put(sEdgeView.get(i), i);
            }
            WQUFPC uf = new WQUFPC(sEdgeView.size());
            // exploit info obtained from s+1
            CCS.getSCCs(s + 1).parallelStream()
                    .map(cc -> cc.stream().map(e -> invMap.get(e)).collect(Collectors.toList()))
                    .forEach(cc -> uf.initializeFromCC(cc));
            // exploit info obtained from larger s values
            CCS.getOverlappingPairs().stream()
                    .forEach(p -> uf.union(invMap.get(p.getValue0()), invMap.get(p.getValue1())));
            // update vertex-hyperedge index
            sEdgeView.stream()
                    .filter(e -> !mask.get(e))
                    .forEach(e -> {
                        getEdge(e).getVertices()
                                .stream()
                                .forEach(v -> {
                                    List<Integer> tmpList = vIndex.getOrDefault(v, Lists.newArrayList());
                                    tmpList.add(e);
                                    vIndex.put(v, tmpList);
                                });
                        mask.set(e);
                    });
            // sort hyperedges by decreasing size
            vIndex.values().forEach(lst -> Collections.sort(lst, (Integer o1, Integer o2)
                    -> -Integer.compare(getNumVerticesOf(o1), getNumVerticesOf(o2))));
            // populate dynamic index
            Map<Integer, Map<Integer, Integer>> eIndex = Maps.newHashMap();
            final int size = s;
            vIndex.keySet().forEach(v -> {
                List<Integer> currentHE = vIndex.get(v);
                int numHEdges = currentHE.size();
                int i, j, v1, v2;
                for (i = 0; i < numHEdges - 1; i++) {
                    v1 = invMap.get(currentHE.get(i));
                    Map<Integer, Integer> tmpI = eIndex.getOrDefault(v1, Maps.newHashMap());
                    for (j = i + 1; j < numHEdges; j++) {
                        v2 = invMap.get(currentHE.get(j));
                        if (!uf.sameCC(v1, v2)) {
                            tmpI.put(v2, tmpI.getOrDefault(v2, 0) + 1);
                            if (tmpI.get(v2) >= size) {
                                CCS.addOverlap(currentHE.get(i), currentHE.get(j), size);
//                                uf.eTvLSP(v1, v2);
                                uf.union(v1, v2);
                            }
                        } else {
                            CCS.addCandidateNeighbourPair(currentHE.get(i), currentHE.get(j), size);
                        }
                    }
                    eIndex.put(v1, tmpI);
                }
            });
            Map<Integer, List<Integer>> thisCCS = sEdgeView.parallelStream()
                    .collect(Collectors.groupingBy(id -> uf.find(invMap.get(id)), Collectors.toList()));
            List<List<Integer>> ccsList = Lists.newArrayList(thisCCS.values());
            CCS.addSCCs(ccsList, s);
            CCS.addMemberships(ccsList, s);
        }
        return CCS;
    }
    
    /**
     * Find all the s-connected components for each s up to maxS, at once.
     * No meta-information is stored. Method used for the comparison with the
     * baselines.
     * 
     * @param maxS max s searched
     * @return the s-connected components for each s not greater than maxS
     */
    public ConnectedComponents simplifiedConnectedComponents(int maxS) {
        // create structures
        ConnectedComponents CCS = new ConnectedComponents();
        Map<Integer, List<Integer>> vIndex = Maps.newHashMap();
        // instantiate BitSet to keep track of processed edges
        BitSet mask = new BitSet(getNumEdges());
        // start from largest s, to exploit neighbouring info
        for (int s = maxS; s > 0; s--) {
            List<Integer> sEdgeView = getEdgesWithMinSize(s);
            Map<Integer, Integer> invMap = Maps.newHashMap();
            for (int i = 0; i < sEdgeView.size(); i++) {
                invMap.put(sEdgeView.get(i), i);
            }
            WQUFPC uf = new WQUFPC(sEdgeView.size());
            // exploit info obtained from s+1
            CCS.getSCCs(s + 1).parallelStream()
                    .map(cc -> cc.stream().map(e -> invMap.get(e)).collect(Collectors.toList()))
                    .forEach(cc -> uf.initializeFromCC(cc));
            // exploit info obtained from larger s values
            CCS.getOverlappingPairs().stream()
                    .forEach(p -> uf.union(invMap.get(p.getValue0()), invMap.get(p.getValue1())));
            // update vertex-hyperedge index
            sEdgeView.stream()
                    .filter(e -> !mask.get(e))
                    .forEach(e -> {
                        getEdge(e).getVertices()
                                .stream()
                                .forEach(v -> {
                                    List<Integer> tmpList = vIndex.getOrDefault(v, Lists.newArrayList());
                                    tmpList.add(e);
                                    vIndex.put(v, tmpList);
                                });
                        mask.set(e);
                    });
            // sort hyperedges by decreasing size
            vIndex.values().forEach(lst -> Collections.sort(lst, (Integer o1, Integer o2)
                    -> -Integer.compare(getNumVerticesOf(o1), getNumVerticesOf(o2))));
            // populate dynamic index
            Map<Integer, Map<Integer, Integer>> eIndex = Maps.newHashMap();
            final int size = s;
            vIndex.keySet().forEach(v -> {
                List<Integer> currentHE = vIndex.get(v);
                int numHEdges = currentHE.size();
                int i, j, v1, v2;
                for (i = 0; i < numHEdges - 1; i++) {
                    v1 = invMap.get(currentHE.get(i));
                    Map<Integer, Integer> tmpI = eIndex.getOrDefault(v1, Maps.newHashMap());
                    for (j = i + 1; j < numHEdges; j++) {
                        v2 = invMap.get(currentHE.get(j));
                        if (!uf.sameCC(v1, v2)) {
                            tmpI.put(v2, tmpI.getOrDefault(v2, 0) + 1);
                            if (tmpI.get(v2) >= size) {
                                CCS.addOverlap(currentHE.get(i), currentHE.get(j), size);
//                                uf.eTvLSP(v1, v2);
                                uf.union(v1, v2);
                            }
                        }
                    }
                    eIndex.put(v1, tmpI);
                }
            });
            Map<Integer, List<Integer>> thisCCS = sEdgeView.parallelStream()
                    .collect(Collectors.groupingBy(id -> uf.find(invMap.get(id)), Collectors.toList()));
            List<List<Integer>> ccsList = Lists.newArrayList(thisCCS.values());
            CCS.addSCCs(ccsList, s);
        }
        return CCS;
    }

    /**
     *
     * @param sHyperEdges list of hyperedges with size not lower than s
     * @param s min overlap size
     * @return s-connected components; for each s-cc, it returns the list of
     * hyperedges in that s-cc
     */
    public List<List<Integer>> findSConnectedComponents(List<Integer> sHyperEdges, int s) {
        WQUFPC uf = new WQUFPC(sHyperEdges.size());
        Map<Integer, List<Integer>> vIndex = IntStream.range(0, sHyperEdges.size())
                .boxed()
                .parallel()
                .flatMap(i -> getEdge(sHyperEdges.get(i)).getVertices()
                .stream()
                .map(v -> new Pair<Integer, Integer>(v, i)))
                .collect(Collectors.groupingBy(id -> id.getValue0(),
                        Collectors.mapping(id -> id.getValue1(), Collectors.toList())));
        // sort hyperedges by decreasing size
        vIndex.values().forEach(lst -> Collections.sort(lst, (Integer o1, Integer o2)
                -> -Integer.compare(getNumVerticesOf(sHyperEdges.get(o1)), getNumVerticesOf(sHyperEdges.get(o2)))));
        Map<Integer, Map<Integer, Integer>> eIndex = Maps.newHashMap();
        vIndex.keySet().forEach(v -> {
            List<Integer> currentHE = vIndex.get(v);
            int numHEdges = currentHE.size();
            int i, j, v1, v2;
            for (i = 0; i < numHEdges; i++) {
                v1 = currentHE.get(i);
                Map<Integer, Integer> tmpI = eIndex.getOrDefault(v1, Maps.newHashMap());
                for (j = i + 1; j < numHEdges; j++) {
                    v2 = currentHE.get(j);
                    if (!uf.sameCC(v1, v2)) {
                        tmpI.put(v2, tmpI.getOrDefault(v2, 0) + 1);
                        if (tmpI.get(v2) >= s) {
//                            uf.eTvLSP(v1, v2);
                            uf.union(v1, v2);
                        }
                    }
                }
                eIndex.put(v1, tmpI);
            }
        });
        Map<Integer, List<Integer>> ccs = IntStream.range(0, sHyperEdges.size())
                .boxed()
                .parallel()
                .collect(Collectors.groupingBy(id -> uf.find(id),
                        Collectors.mapping(id -> sHyperEdges.get(id), Collectors.toList())));
        return Lists.newArrayList(ccs.values());
    }

    /**
     *
     * @param start starting hyperedge
     * @param s min overlap size
     * @return s-distances from start to all the hyperedges reachable from
     * start
     */
    public Map<Integer, Integer> findDistancesFrom(int start, int s) {
        Set<Integer> visited = Sets.newHashSet();
        Map<Integer, Integer> distances = Maps.newHashMap();
        Queue<Pair<Integer, Integer>> queue = new PriorityQueue(new Comparator<Pair<Integer, Integer>>() {
            @Override
            public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                return Integer.compare(o1.getValue1(), o2.getValue1());
            }
        });
        queue.add(new Pair<>(start, 1));
        visited.add(start);
        distances.put(start, 0);
        while (!queue.isEmpty()) {
            Pair<Integer, Integer> entry = queue.poll();
            int eID = entry.getValue0();
            int step = entry.getValue1();
            for (int ngb : hyperedges.get(eID).getSNeighbours(s)) {
                if (visited.add(ngb)) {
                    distances.put(ngb, step);
                    queue.add(new Pair<>(ngb, step + 1));
                }
            }
        }
        return distances;
    }
    
    /**
     *
     * @param start starting hyperedge
     * @param s min overlap size
     * @param labels label of each hyperedge
     * @param maxReached the search stops as soon as maxReached hyperedges with
     * the same label as start have been found
     * @param kind kind of distance to compute, among "edge" (edge to edge),
     * "vertex" (vertex to vertex), and "both" (vertex to edge)
     * @return s-distances from start to all the hyperedges reachable from
     * start, under the maxReached constraint 
     */
    public Map<Integer, Integer> findDistancesFrom(int start, 
            int s, 
            Map<Integer, String> labels, 
            int maxReached,
            String kind) {
    
        Set<Integer> visited = Sets.newHashSet();
        Map<Integer, Integer> distances = Maps.newHashMap();
        String startLabel = (labels != null) ? labels.get(start) : "";
        Queue<Pair<Integer, Integer>> queue = new PriorityQueue(new Comparator<Pair<Integer, Integer>>() {
            @Override
            public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                return Integer.compare(o1.getValue1(), o2.getValue1());
            }
        });
        queue.add(new Pair<>(start, 1));
        visited.add(start);
        distances.put(start, 0);
        int reached = 0;
        while (!queue.isEmpty() && reached < maxReached) {
            Pair<Integer, Integer> entry = queue.poll();
            int eID = entry.getValue0();
            int step = entry.getValue1();
            for (int ngb : hyperedges.get(eID).getSNeighbours(s)) {
                if (visited.add(ngb)) {
                    distances.put(ngb, step);
                    if (labels == null || 
                            !kind.equalsIgnoreCase("vertex") && labels.get(ngb).equalsIgnoreCase(startLabel) ||
                            kind.equalsIgnoreCase("vertex") && getEdge(ngb).getVertices().stream().anyMatch(v -> labels.get(v).equalsIgnoreCase(startLabel))) {
                        reached += 1;
                    }
                    queue.add(new Pair<>(ngb, step + 1));
                }
            }
        }
        return distances;
    }

    /**
     * Bidirectional BFS to find s-distance between hyperedges
     *
     * @param a source hyperedge
     * @param b destination hyperedge
     * @param s min overlap between consecutive hyperedges
     * @param estSize estimated size of the component including a and b
     * @return s-distance between a and b
     */
    public Set<Integer> bidirectionalSPSearch(int a, int b, int s, int estSize) {
        Set<Integer> path = Sets.newHashSet();
        if (a == b) {
            path.add(a);
            return path;
        }
        Tree treeA = new Tree(estSize);
        Tree treeB = new Tree(estSize);
        ArrayDeque<Pair<Integer, Integer>> queueA = new ArrayDeque<>(getNumEdges() / 2);
        ArrayDeque<Pair<Integer, Integer>> queueB = new ArrayDeque<>(getNumEdges() / 2);

        queueA.add(new Pair<>(a, 0));
        treeA.addNode(a, -1);
        queueB.add(new Pair<>(b, 0));
        treeB.addNode(b, -1);
        int frontier = 0;

        while (!queueA.isEmpty() && !queueB.isEmpty()) {
            if (stepForward(queueA, treeA, treeB, s, frontier)) {
                path = treeA.treeIntersect(treeB);
                if (!path.isEmpty()) {
                    return path;
                }
            }
            if (stepForward(queueB, treeB, treeA, s, frontier)) {
                path = treeB.treeIntersect(treeA);
                if (!path.isEmpty()) {
                    return path;
                }
            }
            frontier += 1;
        }
        return path;
    }

    /**
     *
     * @param queue hyperedges left to examine
     * @param visited hyperedges visited from source
     * @param s min overlap size
     * @param frontier distance from source of the hyperedges in the frontier
     * @return true if some hyperedge has already been reached from the
     * destination
     */
    private boolean stepForward(ArrayDeque<Pair<Integer, Integer>> queue,
            Tree visitedFromSrc,
            Tree visitedFromDst,
            int s, int frontier) {

        boolean intersect = false;
        while (!queue.isEmpty()) {
            if (queue.peek().getValue1() > frontier) {
                break;
            }
            Pair<Integer, Integer> p = queue.poll();
            int eID = p.getValue0();
            for (int ngb : hyperedges.get(eID).getSNeighbours(s)) {
                if (!visitedFromSrc.hasBeenVisited(ngb, true)) {
                    visitedFromSrc.addNode(ngb, eID);
                    queue.add(new Pair<>(ngb, p.getValue1() + 1));
                }
                if (visitedFromDst.hasBeenVisited(ngb, false)) {
                    intersect = true;
                }
            }
        }
        return intersect;
    }

    /**
     *
     * @param e1 starting hyperedge
     * @param e2 ending hyperedge
     * @param s min overlap between consecutive hyperedges
     * @param estSize estimated size of the component including a and b
     * @return all the s-paths between e1 and e2
     */
    public List<Set<Integer>> findAllPathsBetween(int e1, int e2, int s, int estSize) {
        List<Set<Integer>> paths = Lists.newArrayList();
        if (e1 == e2) {
            Set<Integer> path = Sets.newHashSet();
            path.add(e1);
            paths.add(path);
            return paths;
        }
        Lattice tree = new Lattice(estSize);
        Map<Integer, Integer> visited = Maps.newHashMap();
        Queue<Pair<Integer, Integer>> queue = new ArrayDeque();
        queue.add(new Pair<>(e1, 1));
        visited.put(e1, 1);
        tree.addNode(e1, -1);
        while (!queue.isEmpty()) {
            Pair<Integer, Integer> entry = queue.poll();
            int eID = entry.getValue0();
            int step = entry.getValue1();
            for (int ngb : hyperedges.get(eID).getSNeighbours(s)) {
                // if I have not visited ngb or I have visited it through a 
                // path with length equal to the current one (i.e., they are
                // both shortest paths)
                if (!visited.containsKey(ngb)) {
                    visited.put(ngb, step + 1);
                    tree.addNode(ngb, eID);
                    if (ngb != e2) {
                        queue.add(new Pair<>(ngb, step + 1));
                    }
                } else if (visited.get(ngb) == step + 1) {
                    tree.addNode(ngb, eID);
                }
            }
        }
        return tree.getPathsToId(e2);
    }
    
    /**
     *
     * @param sample pairs of hyperedges
     * @param maxS max overlap size
     * @param kind kind of distance to compute, among "edge" (edge to edge),
     * "vertex" (vertex to vertex), and "both" (vertex to edge)
     * @return s-distance profiles for the pairs in the sample
     */
    public Map<Pair<Integer, Integer>, DistanceProfile> computeRealDistanceProfilesAmong(
            Collection<Pair<Integer, Integer>> sample,
            int maxS,
            String kind) {
        if (kind.equalsIgnoreCase("edge")) {
            return computeRealEdgeDistanceProfilesAmong(sample, maxS);
        }
        if (kind.equalsIgnoreCase("vertex")) {
            return computeRealVertexDistanceProfilesAmong(sample, maxS);
        }
        if (kind.equalsIgnoreCase("both")) {
            return computeRealVEDistanceProfilesAmong(sample, maxS);
        }
        throw new IllegalArgumentException("Kind " + kind + " not available.");
    }
    
    /**
     *
     * @param sample hyperedges/vertices
     * @param s min overlap size
     * @param kind kind of distance to compute, among "edge" (edge to edge),
     * "vertex" (vertex to vertex), and "both" (vertex to edge)
     * @return s-centrality for each element in the sample
     */
    public Map<Integer, Double> computeSCentralities(
            Collection<Integer> sample,
            int s,
            String kind) {
        if (kind.equalsIgnoreCase("edge")) {
            return computeEdgeSCentrality(sample, s);
        }
        if (kind.equalsIgnoreCase("vertex") || kind.equalsIgnoreCase("both")) {
            return computeVertexSCentrality(sample, s);
        }
        throw new IllegalArgumentException("Kind " + kind + " not available.");  
    }
    
    /**
     *
     * @param queries triplets (src,dest,s) of s-distances to estimate
     * @param kind kind of distance to compute, among "edge" (edge to edge),
     * "vertex" (vertex to vertex), and "both" (vertex to edge)
     * @return s-distance profiles for the (src,dest) pairs in queries
     */
    public Map<Pair<Integer, Integer>, DistanceProfile> computeRealDistanceProfilesAmong(
            Collection<Triplet<Integer, Integer, Integer>> queries,
            String kind) {
        if (kind.equalsIgnoreCase("edge")) {
            return computeRealEdgeDistanceProfilesAmong(queries);
        }
        if (kind.equalsIgnoreCase("vertex")) {
            return computeRealVertexDistanceProfilesAmong(queries);
        }
        if (kind.equalsIgnoreCase("both")) {
            return computeRealVEDistanceProfilesAmong(queries);
        }
        throw new IllegalArgumentException("Kind " + kind + " not available.");
    }

    /**
     *
     * @param sample pairs of hyperedges
     * @param maxS max overlap size
     * @return s-distance profiles for the pairs of hyperedges in the sample
     */
    public Map<Pair<Integer, Integer>, DistanceProfile> computeRealEdgeDistanceProfilesAmong(
            Collection<Pair<Integer, Integer>> sample,
            int maxS) {

        return sample
                .parallelStream()
                .map(pair -> {
                    DistanceProfile dp = new DistanceProfile(pair.getValue0(), pair.getValue1());
                    for (int i = 1; i <= maxS; i++) {
                        int d = bidirectionalSPSearch(pair.getValue0(), pair.getValue1(), i, getNumEdges()).size() - 1;
                        if (d > 0) {
                            dp.addDistance(i, d);
                        }
                    }
                    return new Pair<Pair<Integer, Integer>, DistanceProfile>(pair, dp);
                })
                .collect(Collectors.toMap(k -> k.getValue0(), k -> k.getValue1()));
    }
    
    /**
     *
     * @param queries triplets (src,dest,s) of s-distances to estimate
     * @return s-distance profiles for the pairs of hyperedges in queries
     */
    public Map<Pair<Integer, Integer>, DistanceProfile> computeRealEdgeDistanceProfilesAmong(
            Collection<Triplet<Integer, Integer, Integer>> queries) {

        return queries
                .parallelStream()
                .map(tri -> {
                    DistanceProfile dp = new DistanceProfile(tri.getValue0(), tri.getValue1());
                    int d = bidirectionalSPSearch(
                            tri.getValue0(), 
                            tri.getValue1(), 
                            tri.getValue2(),
                            getNumEdges()).size() - 1;
                    if (d > 0) {
                        dp.addDistance(tri.getValue2(), d);
                    }
                    return new Pair<Pair<Integer, Integer>, DistanceProfile>(new Pair<>(tri.getValue0(), tri.getValue1()), dp);
                })
                .collect(Collectors.toMap(k -> k.getValue0(), k -> k.getValue1()));
    }
    
    /**
     *
     * @param sample hyperedges
     * @param s min overlap size
     * @return s-centrality for the hyperedges in the sample
     */
    public Map<Integer, Double> computeEdgeSCentrality(
            Collection<Integer> sample,
            int s) {
        
        return sample
                .parallelStream()
                .map(e -> {
                    // check if hyperedge has size < s
                    if (getNumVerticesOf(e) < s) {
                        return new Pair<>(e, 0.);
                    }
                    Map<Integer, Integer> distances = findDistancesFrom(e, s);
                    double sum = distances.values().stream().mapToInt(i -> i).sum();
                    int numElem = distances.size() - 1;
                    // map could contain e itself
                    if (distances.containsKey(e)) {
                        numElem --;
                    }
                    if (numElem > 0) {
                        return new Pair<>(e, numElem/sum);
                    }
                    return new Pair<>(e, 0.);
                })
                .collect(Collectors.toMap(k -> k.getValue0(), k -> k.getValue1()));
    }

    /**
     *
     * @param sample pairs of vertices
     * @param maxS max overlap size
     * @return s-distance profiles for the pairs of vertices in the sample
     */
    public Map<Pair<Integer, Integer>, DistanceProfile> computeRealVertexDistanceProfilesAmong(
            Collection<Pair<Integer, Integer>> sample,
            int maxS) {

        return sample
                .parallelStream()
                .map(pair -> {
                    DistanceProfile dp = new DistanceProfile(pair.getValue0(), pair.getValue1());
                    for (int i = 1; i <= maxS; i++) {
                        int d = Integer.MAX_VALUE;
                        for (int e1 : getSHyperEdgesOf(pair.getValue0(), i)) {
                            for (int e2 : getSHyperEdgesOf(pair.getValue1(), i)) {
                                int pathSize = bidirectionalSPSearch(e1, e2, i, getNumEdges()).size() - 1;
                                if (pathSize > 0) {
                                    d = Math.min(d, pathSize);
                                }
                            }
                        }
                        if (d != Integer.MAX_VALUE) {
                            dp.addDistance(i, d);
                        }
                    }
                    return new Pair<Pair<Integer, Integer>, DistanceProfile>(pair, dp);
                })
                .collect(Collectors.toMap(k -> k.getValue0(), k -> k.getValue1()));
    }
    
    /**
     *
     * @param queries triplets (src,dest,s) of s-distances to estimate
     * @return s-distance profiles for the pairs of vertices in queries
     */
    public Map<Pair<Integer, Integer>, DistanceProfile> computeRealVertexDistanceProfilesAmong(
            Collection<Triplet<Integer, Integer, Integer>> queries) {

        return queries
                .parallelStream()
                .map(tri -> {
                    DistanceProfile dp = new DistanceProfile(tri.getValue0(), tri.getValue1());
                    int d = Integer.MAX_VALUE;
                    for (int e1 : getSHyperEdgesOf(tri.getValue0(), tri.getValue2())) {
                        for (int e2 : getSHyperEdgesOf(tri.getValue1(), tri.getValue2())) {
                            int pathSize = bidirectionalSPSearch(e1, e2, tri.getValue2(), getNumEdges()).size() - 1;
                            if (pathSize > 0) {
                                d = Math.min(d, pathSize);
                            }
                        }
                    }
                    if (d != Integer.MAX_VALUE) {
                        dp.addDistance(tri.getValue2(), d);
                    }
                    return new Pair<Pair<Integer, Integer>, DistanceProfile>(new Pair<>(tri.getValue0(), tri.getValue1()), dp);
                })
                .collect(Collectors.toMap(k -> k.getValue0(), k -> k.getValue1()));
    }
    
    /**
     *
     * @param sample vertices
     * @param s min overlap size
     * @return s-centrality for each vertex in the sample
     */
    public Map<Integer, Double> computeVertexSCentrality(
            Collection<Integer> sample,
            int s) {
        
        return sample
                .parallelStream()
                .map(v -> {
                    if (getSHyperEdgesOf(v, s).isEmpty()) {
                        return new Pair<>(v, 0.);
                    }
                    // a vertex can belong to many s-ccs; its s-centrality
                    // value is the max among all the centralities 
                    // (the smallest the value, the more central the vertex)
                    double cent = 0.;
                    for (int e1 : getSHyperEdgesOf(v, s)) {
                        Map<Integer, Integer> thisDistances = findDistancesFrom(e1, s);
                        // compute s-centrality of e1
                        double sum = thisDistances.values().stream().mapToInt(i -> i).sum();
                        int numElem = thisDistances.size() - 1;
                        // map could contain e itself
                        if (thisDistances.containsKey(e1)) {
                            numElem --;
                        }
                        if (numElem > 0) {
                            cent = Math.max(cent, numElem/sum);
                        }
                    }
                    return new Pair<>(v, cent);
                })
                .collect(Collectors.toMap(k -> k.getValue0(), k -> k.getValue1()));
    }
    
    /**
     *
     * @param sample pairs of vertex-hyperedge
     * @param maxS max overlap size
     * @return s-distance profiles from the vertices to the hyperedges in the sample
     */
    public Map<Pair<Integer, Integer>, DistanceProfile> computeRealVEDistanceProfilesAmong(
            Collection<Pair<Integer, Integer>> sample,
            int maxS) {

        return sample
                .parallelStream()
                .map(pair -> {
                    DistanceProfile dp = new DistanceProfile(pair.getValue0(), pair.getValue1());
                    for (int i = 1; i <= maxS; i++) {
                        int d = Integer.MAX_VALUE;
                        for (int e1 : getSHyperEdgesOf(pair.getValue0(), i)) {
                            int pathSize = bidirectionalSPSearch(e1, pair.getValue1(), i, getNumEdges()).size() - 1;
                            if (pathSize > 0) {
                                d = Math.min(d, pathSize);
                            }
                        }
                        if (d != Integer.MAX_VALUE) {
                            dp.addDistance(i, d);
                        }
                    }
                    return new Pair<Pair<Integer, Integer>, DistanceProfile>(pair, dp);
                })
                .collect(Collectors.toMap(k -> k.getValue0(), k -> k.getValue1()));
    }
    
    /**
     *
     * @param queries triplets (src,dest,s) of s-distances to estimate
     * @return s-distance profiles from the vertices to the hyperedges in queries
     */
    public Map<Pair<Integer, Integer>, DistanceProfile> computeRealVEDistanceProfilesAmong(
            Collection<Triplet<Integer, Integer, Integer>> queries) {

        return queries
                .parallelStream()
                .map(tri -> {
                    DistanceProfile dp = new DistanceProfile(tri.getValue0(), tri.getValue1());
                    int d = Integer.MAX_VALUE;
                    for (int e1 : getSHyperEdgesOf(tri.getValue0(), tri.getValue2())) {
                        int pathSize = bidirectionalSPSearch(e1, tri.getValue1(), tri.getValue2(), getNumEdges()).size() - 1;
                        if (pathSize > 0) {
                            d = Math.min(d, pathSize);
                        }
                    }
                    if (d != Integer.MAX_VALUE) {
                        dp.addDistance(tri.getValue2(), d);
                    }
                    return new Pair<Pair<Integer, Integer>, DistanceProfile>(new Pair<>(tri.getValue0(), tri.getValue1()), dp);
                })
                .collect(Collectors.toMap(k -> k.getValue0(), k -> k.getValue1()));
    }
    
    /**
     * Method used in {@link TopReachableWithTag}
     * @param sample a set of hyperedges
     * @param maxD max overlap size
     * @param labels label of each hyperedge
     * @param maxReached max number of hyperedges with the same label that we 
     * want to find
     * @param kind kind of distance to compute, among "edge" (edge to edge),
     * "vertex" (vertex to vertex), and "both" (vertex to edge)
     * @return distance profiles between the hyperedges in the sample and the 
     * reachable hyperedges, found under the maxReached constraint
     */
    public Map<Pair<Integer, Integer>, DistanceProfile> findTopReachableFromWTag(
            List<Integer> sample,
            int maxD,
            Map<Integer, String> labels,
            int maxReached,
            String kind) {
        
        return sample.parallelStream().flatMap(e -> {
            Map<Pair<Integer, Integer>, DistanceProfile> profiles = Maps.newHashMap();
            for (int s = 1; s <= maxD; s++) {
                Map<Integer, Integer> reached = Maps.newHashMap();
                if (kind.equalsIgnoreCase("edge")) {
                    if (getNumVerticesOf(e) < maxD) {
                        continue;
                    }
                    reached = findDistancesFrom(e, s, labels, maxReached, kind);
                } else if (kind.equalsIgnoreCase("vertex")) {
                    for (int e1 : getSHyperEdgesOf(e, s)) {
                        Map<Integer, Integer> reachedFromE = findDistancesFrom(e1, s, labels, maxReached, kind);
                        // find s-distance from e to all the reachable vertices
                        for (Entry<Integer, Integer> entry : reachedFromE.entrySet()) {
                            for (int ngb : getVerticesOf(entry.getKey())) {
                                reached.put(ngb, Math.min(reached.getOrDefault(ngb, Integer.MAX_VALUE), entry.getValue()));
                            }
                        }
                    }
                } else {
                    throw new UnsupportedOperationException("Kind " + kind + " not yet supported.");
                }
                final int thisS = s;
                reached.entrySet().stream().forEach(entry -> {
                    Pair<Integer, Integer> p = new Pair<>(e, entry.getKey());
                    DistanceProfile dp = profiles.getOrDefault(p, new DistanceProfile(e, entry.getKey()));
                    dp.addDistance(thisS, entry.getValue());
                    profiles.put(p, dp);
                });
            }
            return profiles.entrySet().stream();
        }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

     /**
     * Method used in {@link TopKReachable}
     * @param queries query elements
     * @param maxD max overlap size
     * @param k number of closest neighbors to find
     * @param kind kind of distance to compute, among "edge" (edge to edge),
     * "vertex" (vertex to vertex), and "both" (vertex to edge)
     * @return for each s up to maxD, top-k elements s-reachable from each query element
     */
    public Map<Integer, int[][]> findTopKReachable(
            List<Integer> queries,
            int maxD,
            int k,
            String kind) {
        
        Map<Integer, int[][]> topKPerS = Maps.newHashMap();
        for (int s = 1; s <= maxD; s++) {
            int[][] topKPerQuery = new int[queries.size()][k];
            final int thisS = s;
            Map<Integer, int[]> reachables = queries.stream()
                .parallel()
                .map(q -> {
                    Map<Integer, Integer> reached = Maps.newHashMap();
                    int[] sortedReached = new int[k];
                    if (kind.equalsIgnoreCase("edge")) {
                        if (getNumVerticesOf(q) < maxD) {
                            for (int i = 0; i < k; i++) {
                                sortedReached[i] = -1;
                            }
                            return new Pair<Integer, int[]>(q, sortedReached);
                        }
                        reached = findDistancesFrom(q, thisS, null, k, kind);
                    } else {
                        for (int e1 : getSHyperEdgesOf(q, thisS)) {
                            Map<Integer, Integer> reachedFromE = findDistancesFrom(e1, thisS, null, k, kind);
                            // find s-distance from e to all the reachable vertices
                            for (Entry<Integer, Integer> entry : reachedFromE.entrySet()) {
                                if (kind.equalsIgnoreCase("vertex")) {
                                    for (int ngb : getVerticesOf(entry.getKey())) {
                                        reached.put(ngb, 
                                                Math.min(reached.getOrDefault(ngb, Integer.MAX_VALUE), 
                                                        entry.getValue()));
                                    }
                                } else {
                                    reached.put(entry.getKey(), 
                                            Math.min(reached.getOrDefault(entry.getKey(), Integer.MAX_VALUE), 
                                                    entry.getValue()));
                                }
                            }
                        }
                    }
                    List<Entry<Integer, Integer>> topKReached = Lists.newArrayList(reached.entrySet());
                    Collections.sort(topKReached, 
                            (Entry<Integer, Integer> e1, Entry<Integer, Integer> e2) -> {
                                if (e1.getValue().equals(e2.getValue())) {
                                    return Integer.compare(e1.getKey(), e2.getKey());
                                }
                                return Integer.compare(e1.getValue(), e2.getValue());
                            });
                    int maxK = Math.min(k, topKReached.size());
                    for (int i = 0; i < maxK; i++) {
                        sortedReached[i] = topKReached.get(i).getKey();
                    }
                    for (int i = maxK; i < k; i++) {
                        sortedReached[i] = -1;
                    }
                    return new Pair<Integer, int[]>(q, sortedReached);
                })
                .collect(Collectors.toMap(e -> e.getValue0(), e -> e.getValue1()));
            for (int i = 0; i < queries.size(); i++) {
                topKPerQuery[i] = reachables.get(queries.get(i));
            }
            topKPerS.put(thisS, topKPerQuery);
        }
        return topKPerS;
    }

    public void printVertexMap() {
        vertexMap.entrySet().forEach(e -> {
            System.out.print("V:" + e.getKey() + "\t");
            System.out.println("MEMB:" + e.getValue().toString());
        });
    }

    public void printNeighbours() {
        hyperedges.stream().forEach(edge -> {
            System.out.println("E:" + edge.toString());
            edge.getNeighbourData()
                    .entrySet()
                    .forEach(n -> System.out.println("("
                    + hyperedges.get(n.getKey()).toString()
                    + "," + n.getValue()
                    + ")"));
        });
    }

    public void printNumNeighbours() {
        hyperedges.stream().forEach(edge
                -> System.out.println(edge.getId() + "," + edge.getNumSNeighbours(1)));
    }

}
