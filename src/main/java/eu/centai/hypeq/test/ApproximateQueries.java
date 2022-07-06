package eu.centai.hypeq.test;

import eu.centai.hypeq.oracle.structures.DistanceOracle;
import eu.centai.hypeq.oracle.structures.DistanceProfile;
import eu.centai.hypeq.utils.CMDLParser;
import eu.centai.hypeq.utils.Reader;
import eu.centai.hypeq.utils.Settings;
import eu.centai.hypeq.utils.StopWatch;
import eu.centai.hypeq.utils.Writer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Pair;

/**
 * Class used to evaluate the performance and the accuracy of the oracle, when 
 * answering edge-to-edge queries in the case of pre-saved oracle and query pairs.
 * 
 * @author giulia
 */
public class ApproximateQueries {
    
    /**
     * 
     * @param oracle distance oracle
     * @param sample pairs of hyperedges
     * @return distance profiles for all the pairs in the sample
     * @throws IOException 
     */
    public static Map<Pair<Integer, Integer>, DistanceProfile> populateHEDistanceProfiles(
            DistanceOracle oracle,
            Collection<Pair<Integer, Integer>> sample) throws IOException {
        
        Map<Pair<Integer, Integer>, DistanceProfile> allApproxHDist = sample.parallelStream()
                .map(entry -> {
                    int u = entry.getValue0();
                    int v = entry.getValue1();
                    DistanceProfile dp = new DistanceProfile(u, v);
                    for (int s = 1; s <= Settings.maxS; s++) {
                        dp.addDistance(s, oracle.getApproxSDistanceBetween(u, v, s, Settings.lb));
                    }
                    return new Pair<Pair<Integer, Integer>, DistanceProfile>(entry, dp);
                })
                .collect(Collectors.toMap(e -> e.getValue0(), e -> e.getValue1()));
        return allApproxHDist;
    }
    
    public static void main(String[] args) throws Exception {
        //parse the command line arguments
        CMDLParser.parse(args);
        // load oracle
        DistanceOracle oracle = null;
        try {
            oracle = Reader.readOracle();
        } catch (FileNotFoundException ex) {
            System.out.println("oracle file not found.");
            System.exit(1);
        }
        // sample some hyperedges
        System.out.println("loading sample...");
        Set<Pair<Integer, Integer>> sample = Reader.readHyperEdgeSample();
        // query time and approximations
        System.out.println("finding approx distances for " + sample.size() + " pairs...");
        StopWatch watch = new StopWatch();
        watch.start();
        Map<Pair<Integer, Integer>, DistanceProfile> approxDist = populateHEDistanceProfiles(oracle, sample);
        Writer.writeStats(sample.size(), oracle.getNumLandmarks(), 0, watch.getElapsedTime(), oracle.getOracleSize());
        System.out.println("found distances for " + approxDist.size() + " pairs.");
        Writer.writeResults(null, approxDist, Settings.kind + Settings.seed);
    }
    
}
