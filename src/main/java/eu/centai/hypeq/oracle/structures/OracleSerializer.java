package eu.centai.hypeq.oracle.structures;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.IntArraySerializer;
import com.google.common.collect.Maps;
import java.util.Map;

public class OracleSerializer extends Serializer<DistanceOracle> {

    @Override
    public void write(Kryo kryo, Output output, DistanceOracle oracle) {
        SDistanceOracle[] oracles = oracle.getSOracles();
        output.writeInt(oracles.length);
        for (SDistanceOracle so : oracles) {
            kryo.writeObject(output, so);
        }
        writeComponentSizes(output, kryo, oracle.getCCsSizes());
        writeMemberships(output, kryo, oracle.getCCsMemberships());
    }

    @Override
    public DistanceOracle read(Kryo kryo, Input input, Class<? extends DistanceOracle> type) {
        DistanceOracle o = new DistanceOracle();
        int numOracles = input.readInt();
        SDistanceOracle[] oracles = new SDistanceOracle[numOracles];
        for (int i = 0; i < numOracles; i++) {
            oracles[i] = (SDistanceOracle) kryo.readObject(input, SDistanceOracle.class);
        }
        o.setSOracles(oracles);
        o.setCCsSizes(readComponentSizes(input, kryo));
        o.setCCsMemberships(readMemberships(input, kryo));
        return o;
    }
    
    private void writeComponentSizes(Output out, Kryo kryo, Map<Integer, int[]> sizes) {
        IntArraySerializer ser = new IntArraySerializer();
        out.writeInt(sizes.size());
        sizes.entrySet().stream().forEach(entry -> {
            out.writeInt(entry.getKey());
            ser.write(kryo, out, entry.getValue());
        });
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
    
    private Map<Integer, int[]> readComponentSizes(Input input, Kryo kryo) {
        IntArraySerializer ser = new IntArraySerializer();
        Map<Integer, int[]> ccs = Maps.newHashMap();
        int numS = input.readInt();
        for (int j = 0; j < numS; j++) {
            int key = input.readInt();
            int[] cc = ser.read(kryo, input, int[].class);
            ccs.put(key, cc);
        }
        return ccs;
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
