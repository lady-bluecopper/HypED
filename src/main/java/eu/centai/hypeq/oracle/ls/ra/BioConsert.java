package eu.centai.hypeq.oracle.ls.ra;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import eu.centai.hypeq.utils.Utils;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;
import org.javatuples.Triplet;

/**
 *
 * @author giulia
 */
public class BioConsert {
    
    /**
     * Find a consensus ranking minimizing the disagreements between the rankings:
     * 1) i is ranked before j in one ranking, and after element j in another one;
     * 2) i and j are ties in one ranking, but not in another one.
     * The generalized Kendall-tau distance allows the minimization of both disagreements.
     * The algorithm applies changes to each original ranking.
     * Finally, the best modified ranking is returned as median ranking.
     * 
     * @param sizeScores size of each connected component
     * @param vertexScores num of vertices of each connected component
     * @param sScores s value of each connected component
     * @param landsScores number of landmarks assigned to each connected component
     * @param importance importance factor for each ranking
     * @return median ranking
     */
    public static List<List<Integer>> computeMedianRanking(
            Int2ObjectOpenHashMap<IntOpenHashSet> sizeScores,
            Int2ObjectOpenHashMap<IntOpenHashSet> vertexScores,
            Int2ObjectOpenHashMap<IntOpenHashSet> sScores,
            Int2ObjectOpenHashMap<IntOpenHashSet> landsScores,
            double[] importance) {

        // from scores to rankings
        // each ranking is a map of buckets, where each bucket includes 
        // elements in the same position in that ranking
        Int2ObjectOpenHashMap<int[]> rankBySize = fromScoreToRank(sizeScores, true);
        Int2ObjectOpenHashMap<int[]> rankByVSize = fromScoreToRank(vertexScores, true); 
        Int2ObjectOpenHashMap<int[]> rankByS = fromScoreToRank(sScores, true);
        Int2ObjectOpenHashMap<int[]> rankByLands = fromScoreToRank(landsScores, false);
        Int2ObjectOpenHashMap<int[]>[] rankings = new Int2ObjectOpenHashMap[]{
            rankBySize,
            rankByVSize,
            rankByS,
            rankByLands
        };
        // map element to canonical id
        Int2IntOpenHashMap elem_id = new Int2IntOpenHashMap();
        // map canonical id to element
        Int2IntOpenHashMap id_elements = new Int2IntOpenHashMap();
        // get distinct items
        rankBySize.values().stream().forEach(rank -> {
            for (int v : rank) {
                elem_id.putIfAbsent(v, elem_id.size());
                id_elements.put(elem_id.get(v), v);
            }
        });
        if (elem_id.isEmpty()) {
            return Lists.newArrayList();
        }
        if (elem_id.size() == 1) {
            List<List<Integer>> out = Lists.newArrayList();
            List<Integer> bucket = Lists.newArrayList(elem_id.keySet());
            out.add(bucket);
            return out;
        }
        // unifying process: an element that does not appear in some ranking,
        // receives rank = max_rank + 1 in that ranking
        // each row is a ranking in rankings
        int[][] departure = departureRankings(rankings, elem_id);
        // cost of each operation for each pair of elements
        double[][][] M = costMatrix(departure, importance);
        // stores best ranking so far
        int id_dst_min = 0;
        // distance from current best ranking to all the rankings
        double dst_min = Double.POSITIVE_INFINITY;
        // strings corresponding to rankings
        Set<String> memoi = Sets.newHashSet();
        for (int id = 0; id < departure.length; id++) {
            // stringify it
            String ranking_string = Arrays.toString(departure[id]);
            // check if this ranking has already been considered
            if (memoi.add(ranking_string)) {
                // find distance from this ranking to all the other rankings
                double dst_ranking = bioConsert(departure[id], M, memoi);
                memoi.add(Arrays.toString(departure[id]));
                if (dst_ranking < dst_min) {
                    // replace current best
                    id_dst_min = id;
                    dst_min = dst_ranking;
                }
            }
        }
        // for each position in the best ranking, 
        // stores the elements in that position
        Map<Integer, List<Integer>> ranking_dict = Maps.newHashMap();
        for (int el = 0; el < M.length; el++) {
            int id_bucket = departure[id_dst_min][el];
            List<Integer> tmp = ranking_dict.getOrDefault(id_bucket, Lists.newArrayList());
            tmp.add(id_elements.get(el));
            ranking_dict.put(id_bucket, tmp);
        }
        // return ranking as list of buckets
        return ranking_dict.keySet()
                .stream()
                .sorted()
                .map(k -> ranking_dict.get(k)).collect(Collectors.toList());
    }

    /**
     * Applies changes to a given ranking, to make it as close as possible to 
     * the median ranking. 
     * Then, it returns the distance from the modified ranking to all the 
     * other rankings.
     * 
     * @param ranking a ranking
     * @param cost_matrix cost matrix
     * @param memoi rankings already explored
     * @return distance from modified ranking to all the others.
     */
    private static double bioConsert(int[] ranking,
            double[][][] cost_matrix, 
            Set<String> memoi) {
        // max position in ranking
        int max_id_bucket = Ints.max(ranking);
        // initial distance from ranking to all the others
        double sum_before = 0.0;
        double sum_tied = 0.0;
        for (int el1 = 0; el1 < ranking.length; el1++) {
            for (int el2 = 0; el2 < ranking.length; el2++) {
                if (ranking[el2] < ranking[el1]) {
                    // number of times el2 is not before el1
                    sum_before += cost_matrix[el1][el2][1]; 
                } else if (ranking[el2] == ranking[el1]) {
                    // number of times el1 and el2 have not the same position
                    sum_tied += cost_matrix[el1][el2][2];
                }
            }
        }
        // sum_tied / 2 because each pair is counted twice
        double dst = sum_before + sum_tied / 2;
        // modify the ranking, as long as the changes improve the ranking 
        boolean improvement = true;
        int to;
        double cost_diff;
        int step = 0;
        while (improvement) {
            step++;
            improvement = false;
            for (int el = 0; el < ranking.length; el++) {
                // current position of el in the ranking
                int bucket_elem = ranking[el];
                // true if el is alone in the bucket
                Triplet<Double[], Double[], Boolean> dc = computeDeltaCosts(ranking, el, cost_matrix, max_id_bucket);
                // delta cost for changing bucket for element el
                Double[] cha = dc.getValue0();
                Double[] add = dc.getValue1();
                Boolean alone = dc.getValue2();
                // find a good change bucket operation
                Pair<Integer, Double> p = searchToChangeBucket(bucket_elem, cha, max_id_bucket);
                // new bucket id for el
                to = p.getValue0();
                cost_diff = p.getValue1(); 
                if (to >= 0) {
                    improvement = true;
                    dst += cost_diff;
                    changeBucket(ranking, el, bucket_elem, to, alone);
                    if (alone) {
                        max_id_bucket -= 1;
                    }
                    dc = computeDeltaCosts(ranking, el, cost_matrix, max_id_bucket);
                    add = dc.getValue1();
                    alone = dc.getValue2();
                    String encoding = Arrays.toString(ranking);
                    if (memoi.contains(encoding)){
                        return dst;
                    }
                    memoi.add(encoding);
                }
                // find a good add bucket operation
                p = searchToAddBucket(bucket_elem, add, max_id_bucket);
                // new bucket id for el
                to = p.getValue0();
                cost_diff = p.getValue1(); 
                if (to >= 0) {
                    improvement = true;
                    dst += cost_diff;
                    addBucket(ranking, el, bucket_elem, to, alone);
                    String encoding = Arrays.toString(ranking);
                    if (!alone) {
                        max_id_bucket += 1;
                    }
                    if (memoi.contains(encoding)){
                        return dst;
                    }
                    memoi.add(encoding);
                }
            }
        }
        return dst;
    }

    /**
     * Initialize delta costs of changing the bucket of element and of adding the
     * element to another bucket, i.e.,
     * delta_change[i] cost of putting element in bucket i
     * delta_add[i] cost of adding a bucket in position i
     * 
     * @param ranking ranking
     * @param element element
     * @param cost_matrix cost matrix
     * @param max_id_bucket max position in ranking
     * @param delta_change delta cost of changing the bucket of element 
     * @param delta_add delta cost of adding the element to another bucket
     * @return true if element is alone in the bucket; false otherwise 
     */
    private static Triplet<Double[], Double[], Boolean> computeDeltaCosts(int[] ranking, 
            int element, 
            double[][][] cost_matrix, 
            int max_id_bucket) {
        
        Double[] delta_change = new Double[max_id_bucket + 2];
        Arrays.fill(delta_change, 0.);
        Double[] delta_add = new Double[max_id_bucket + 3];
        Arrays.fill(delta_add, 0.);
        // current position of element
        int id_bucket_element = ranking[element];
        // elements in the same bucket as element
        List<Integer> same = Lists.newArrayList();

        for (int el = 0; el < ranking.length; el++) {
            int bucket = ranking[el];
            // el is before element
            if (bucket < id_bucket_element) { 
                // delta = put_tied - put_after
                delta_change[bucket] += cost_matrix[element][el][2] - cost_matrix[element][el][1];
                int id = bucket;
                if (bucket == 0) {
                    id = delta_change.length;
                }
                delta_change[id - 1] += cost_matrix[element][el][0] - cost_matrix[element][el][2];
                // delta = put_before - put_after
                delta_add[bucket] += cost_matrix[element][el][0] - cost_matrix[element][el][1];
            // el is after element
            } else if (bucket > id_bucket_element) {
                // delta = put_tied - put_before
                delta_change[bucket] += cost_matrix[element][el][2] - cost_matrix[element][el][0];
                // delta = put_after - put_tied
                delta_change[bucket + 1] += cost_matrix[element][el][1] - cost_matrix[element][el][2];
                // delta = put_after - put_before
                delta_add[bucket + 1] += cost_matrix[element][el][1] - cost_matrix[element][el][0]; 
            // el and element are in the same bucket
            } else {
                same.add(el);
            }
        }
        // cost of leaving a bucket
        double[] leave_bucket = new double[3];
        for (int i : same) {
            for (int j = 0; j < leave_bucket.length; j++) {
                leave_bucket[j] += cost_matrix[element][i][j];
            }
        }
        int id = id_bucket_element;
        if (id_bucket_element == 0) {
            id = delta_change.length;
        }
        delta_change[id - 1] += leave_bucket[0] - leave_bucket[2];
        delta_change[id_bucket_element + 1] += leave_bucket[1] - leave_bucket[2];
        delta_add[id_bucket_element] += leave_bucket[0] - leave_bucket[2];
        delta_add[id_bucket_element + 1] += leave_bucket[1] - leave_bucket[2];

        Double[] cumSum1 = Utils.cumSum(Arrays.copyOfRange(delta_change, 0, id_bucket_element));
        Double[] cumSum2 = Utils.cumSum(Arrays.copyOfRange(delta_change, id_bucket_element, max_id_bucket + 1));
        Double[] cumSum3 = Utils.cumSum(Arrays.copyOfRange(delta_add, 0, id_bucket_element + 1));
        Double[] cumSum4 = Utils.cumSum(Arrays.copyOfRange(delta_add, id_bucket_element + 1, max_id_bucket + 2));
        
        System.arraycopy(cumSum1, 0, delta_change, 0, id_bucket_element);
        System.arraycopy(cumSum2, 0, delta_change, id_bucket_element, cumSum2.length);
        System.arraycopy(cumSum3, 0, delta_add, 0, id_bucket_element + 1);
        System.arraycopy(cumSum4, 0, delta_add, id_bucket_element + 1, cumSum4.length);
        
        delta_change[delta_change.length - 1] = -1.;
        delta_add[delta_add.length - 1] = -1.;
        return new Triplet<>(delta_change, delta_add, same.size() == 1);
    }
    
    /**
     * 
     * @param buck_elem position of element
     * @param change_costs cost difference for each bucket change for element
     * @param max_id_bucket max position in ranking
     * @return new position for element, if a change can decrease the score;
     * -1 otherwise
     */
    private static Pair<Integer, Double> searchToChangeBucket(int buck_elem, 
            Double[] change_costs, 
            int max_id_bucket) {
        
        double max_cost = 0;
        int new_pos = max_id_bucket + 1;
        // search at the right of the current position
        for (int i = 0; i < change_costs.length; i++) {
            if (change_costs[i] < 0 && change_costs[i] < max_cost) {
                max_cost = change_costs[i];
                new_pos = i;
            }
        }
        if (new_pos != buck_elem && new_pos <= max_id_bucket) {
            return new Pair<>(new_pos, change_costs[new_pos]);
        }
        return new Pair<>(-1, 0.);
    }

    /**
     * 
     * @param ranking ranking
     * @param element element
     * @param old_pos old position of element in ranking
     * @param new_pos new position of element in ranking
     * @param alone_in_old_bucket if element is alone in the current bucket
     */
    private static void changeBucket(int[] ranking, 
            int element, int old_pos, int new_pos,
            boolean alone_in_old_bucket) {

        ranking[element] = new_pos;
        // if element is the only one in bucket old_pos
        // all the elements ranked below old_pos go up one position
        if (alone_in_old_bucket) {
            for (int i = 0; i < ranking.length; i++) {
                if (ranking[i] > old_pos) {
                    ranking[i] -= 1;
                }
            }
        }
    }
    
    /**
     * 
     * @param buck_elem position of element
     * @param add_costs cost difference for each bucket add for element
     * @param max_id_bucket max position in ranking
     * @return new position of element if a change can decrease the score; 
     * -1 otherwise
     */
    private static Pair<Integer, Double> searchToAddBucket(int buck_elem, 
            Double[] add_costs,
            int max_id_bucket) {

        double max_cost = 0;
        int new_pos = -1;
        for (int i = 0; i < add_costs.length; i++) {
            if (add_costs[i] < 0 && add_costs[i] < max_cost) {
                max_cost = add_costs[i];
                new_pos = i;
            }
        }
        if (new_pos != buck_elem && new_pos <= max_id_bucket + 1) {
            return new Pair<>(new_pos, add_costs[new_pos]);
        }
        return new Pair<>(-1, 0.);
    }

    /**
     * 
     * @param ranking ranking
     * @param element element
     * @param old_pos old position of element in ranking
     * @param new_pos new position of element
     * @param alone_in_old_bucket if element is alone in the current bucket
     */
    private static void addBucket(int[] ranking,
            int element, 
            int old_pos, 
            int new_pos,
            boolean alone_in_old_bucket) {

        if (!alone_in_old_bucket) {
            for (int j = 0; j < ranking.length; j++) {
                if (ranking[j] >= new_pos) {
                    ranking[j] += 1;
                }
            }
            ranking[element] = new_pos;
            return;
        }
        if (old_pos < new_pos) {
            for (int j = 0; j < ranking.length; j++) {
                if (ranking[j] > old_pos && ranking[j] < new_pos) {
                    ranking[j] -= 1;
                }
            }
            ranking[element] = new_pos - 1;
        } else {
            for (int j = 0; j < ranking.length; j++) {
                if (ranking[j] >= new_pos && ranking[j] < old_pos) {
                    ranking[j] += 1;
                }
            }
            ranking[element] = new_pos;
        }
    }
    
    /**
     * 
     * @param rankings list of rankings; each ranking is a map of buckets; each 
     * bucket is a tie.
     * @param elements_id map element -> canonical id
     * @return a matrix with all the rankings where the element ids are replaced 
     * with their canonical ids
     */
    private static int[][] departureRankings(
            Int2ObjectOpenHashMap<int[]>[] rankings, 
            Int2IntOpenHashMap elements_id) {
        // number of rankings
        int m = rankings.length;
        // number of distinct elements in the rankings
        int n = elements_id.size();
        // starting rankings
        int[][] departure = new int[m][n];
        // iterate over all the rankings
        for (int id = 0; id < m; id++) {
            Int2ObjectOpenHashMap<int[]> ranking = rankings[id];
            int maxRank = Collections.max(ranking.keySet()) + 1;
            final int rid = id;
            Arrays.fill(departure[id], maxRank);
            ranking.int2ObjectEntrySet()
                    .stream()
                    .forEach(bucket -> {
                        for (int element : bucket.getValue()) {
                            // set corresponding ranking
                            departure[rid][elements_id.get(element)] = bucket.getIntKey();
                        }
                    });
        }
        return departure;
    }
    
    /**
     * 
     * @param departures m * n matrix with n items and m starting ranking
     * @param relevance importance factor for each ranking
     * @return cost matrix, where position (i,j) indicates the cost of 
     * (i) putting i before j, (ii) putting i after j, and (iii) putting
     * i and j in the same position
     */
    private static double[][][] costMatrix(int[][] departures, double[] relevance) {
        // number of elements
        int n = departures[0].length;
        // number of rankings
        int m = departures.length;
        // cost matrix
        double[][][] cost_matrix = new double[n][n][3];
        for (int e1 = 0; e1 < n; e1++) {
            // for each distinct pair of elements (e1, e2) with e1 < e2
            for (int e2 = e1 + 1; e2 < n; e2++) {
                int f = 0;
                int b = 0;
                int e = 0;
                for (int i = 0; i < m; i++) {
                    // e1 and e2 are tied in the i-th ranking
                    if (departures[i][e1] == departures[i][e2]) {
                        b += relevance[i];
                    }
                    // e1 is ranked before e2 in the i-th ranking
                    if (departures[i][e1] < departures[i][e2]) {
                        e+= relevance[i];
                    } 
                    // e1 is ranked after e2 in the i-th ranking
                    if (departures[i][e1] > departures[i][e2]) {
                        f+= relevance[i];
                    }
                }
                // total cost of ranking e1 before e2 is the number of times 
                // e1 is not before e2
                double put_before = f + b;
                // total cost of ranking e2 before e1 is the number of times 
                // e2 is not before e1
                double put_after = e + b;
                // total cost of ranking e1 and e2 in the same position is the 
                // number of times they are not ranked in the same position
                double put_tied = e + f;
                cost_matrix[e1][e2] = new double[]{put_before, put_after, put_tied};
                cost_matrix[e2][e1] = new double[]{put_after, put_before, put_tied};
            }
        }
        return cost_matrix;
    }
    
     /**
     * 
     * @param scores map score -> elements with that score
     * @param reverse if higher scores indicate higher ranks
     * @return ranking of the elements based on their score
     */
    private static Int2ObjectOpenHashMap<int[]> fromScoreToRank(
            Int2ObjectOpenHashMap<IntOpenHashSet> scores, 
            boolean reverse) {
        int[] scoreList = Ints.toArray(scores.keySet());
        Arrays.sort(scoreList);
        
        Int2ObjectOpenHashMap<int[]> ranking = new Int2ObjectOpenHashMap();
        if (reverse) {
            for (int i = scoreList.length - 1; i >= 0; i--) {
                ranking.put(i, Ints.toArray(scores.get(scoreList[i])));
            }
            return ranking;
        }
        for (int i = 0; i < scoreList.length; i++) {
            ranking.put(i, Ints.toArray(scores.get(scoreList[i])));
        }
        return ranking;
    }
    
}
