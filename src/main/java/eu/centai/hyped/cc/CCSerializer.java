package eu.centai.hyped.cc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;

/**
 *
 * @author u0m0518
 */
public class CCSerializer extends Serializer<ConnectedComponents> {

    @Override
    public void write(Kryo kryo, Output output, ConnectedComponents t) {
        // write connected components
        writeConnectedComponents(output, kryo, t.getAllSCCs());
        // write partial overlaps
        writeOverlaps(output, kryo, t.getOverlaps());
        // write candidate neighbours
        writeCandNeighs(output, kryo, t.getCandsNeigh());
        // write hyperedge - ccs memberships
        writeMemberships(output, kryo, t.getCCPerHyperEdge());
    }

    @Override
    public ConnectedComponents read(Kryo kryo, Input input, Class<? extends ConnectedComponents> type) {
        ConnectedComponents CCS = new ConnectedComponents();
        CCS.setAllSCCs(readConnectedComponents(input, kryo));
        CCS.setOverlaps(readOverlaps(input, kryo));
        CCS.setCandsNeigh(readCandNeighs(input, kryo));
        CCS.setCCPerHyperedge(readMemberships(input, kryo));
        return CCS;
    }
    
    private void writeConnectedComponents(Output out, Kryo kryo, Map<Integer, List<List<Integer>>> ccs) {
        out.writeInt(ccs.size());
        ccs.entrySet().stream().forEach(entry -> {
            out.writeInt(entry.getKey());
            out.writeInt(entry.getValue().size());
            entry.getValue().stream()
                    .forEach(lst -> kryo.writeObject(out, lst, kryo.getSerializer(java.util.ArrayList.class)));
        });
    }
    
    private void writeOverlaps(Output out, Kryo kryo, Map<Pair<Integer, Integer>, Integer> overlaps) {
        out.writeInt(overlaps.size());
        overlaps.entrySet().forEach(e -> {
            kryo.writeObject(out, e.getKey());
            out.writeInt(e.getValue());
        });
    }
    
    private void writeCandNeighs(Output out, Kryo kryo, Collection<Map<Integer, List<Integer>>> cands) {
        out.writeInt(cands.size());
        for (Map<Integer, List<Integer>> map : cands) {
            out.writeInt(map.size());
            map.entrySet().forEach(entry -> {
                out.writeInt(entry.getKey());
                kryo.writeObject(out, entry.getValue(), kryo.getSerializer(java.util.ArrayList.class));
            });
        }
    }
    
    private void writeMemberships(Output out, Kryo kryo, Map<Integer, Map<Integer, Integer>> memb) {
        MapSerializer ser = new MapSerializer();
        ser.setKeyClass(Integer.class);
        ser.setValueClass(Integer.class);
        out.writeInt(memb.size());
        memb.entrySet().forEach(entry -> {
            out.writeInt(entry.getKey());
            kryo.writeObject(out, entry.getValue(), ser);
        });
    }
    
    private Map<Integer, List<List<Integer>>> readConnectedComponents(Input input, Kryo kryo) {
        Map<Integer, List<List<Integer>>> ccs = Maps.newHashMap();
        int numS = input.readInt();
        for (int j = 0; j < numS; j++) {
            List<List<Integer>> cc = Lists.newArrayList();
            int key = input.readInt();
            int lstSize = input.readInt();
            for (int i = 0; i < lstSize; i++) {
                cc.add((List<Integer>) kryo.readObject(input, java.util.ArrayList.class));
            }
            ccs.put(key, cc);
        }
        return ccs;
    }
    
    private Map<Pair<Integer, Integer>, Integer> readOverlaps(Input input, Kryo kryo) {
        int size = input.readInt();
        Map<Pair<Integer, Integer>, Integer> overlaps = Maps.newHashMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            Pair<Integer, Integer> p = kryo.readObject(input, org.javatuples.Pair.class);
            overlaps.put(p, input.readInt());
        }
        return overlaps;
    }
    
    private List<Map<Integer, List<Integer>>> readCandNeighs(Input input, Kryo kryo) {
        List<Map<Integer, List<Integer>>> cands = Lists.newArrayList();
        int numLists = input.readInt();
        for (int i = 0; i < numLists; i++) {
            int mapSize = input.readInt();
            Map<Integer, List<Integer>> map = Maps.newHashMap();
            for (int j = 0; j < mapSize; j++) {
                int key = input.readInt();
                map.put(key, (List<Integer>) kryo.readObject(input, java.util.ArrayList.class));
            }
            cands.add(map);
        }
        return cands;
    }
    
    private Map<Integer, Map<Integer, Integer>> readMemberships(Input input, Kryo kryo) {
        MapSerializer ser = new MapSerializer();
        ser.setKeyClass(Integer.class);
        ser.setValueClass(Integer.class);
        int numS = input.readInt();
        Map<Integer, Map<Integer, Integer>> memberships = Maps.newHashMap();
        for (int j = 0; j < numS; j++) {
            int key = input.readInt();
            memberships.put(key, (Map<Integer, Integer>) kryo.readObject(input, java.util.HashMap.class, ser));
        }
        return memberships;
    }
    
}
