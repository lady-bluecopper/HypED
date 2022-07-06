package eu.centai.hypeq.oracle.structures;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Map;

public class SOracleSerializer extends Serializer<SDistanceOracle> {

    @Override
    public void write(Kryo kryo, Output output, SDistanceOracle oracle) {
        output.writeInt(oracle.getNumLandmarks());
        kryo.writeObject(output, oracle.getLabels());
    }

    @Override
    public SDistanceOracle read(Kryo kryo, Input input, Class<? extends SDistanceOracle> type) {
        SDistanceOracle o = new SDistanceOracle();
        o.setNumLandmarks(input.readInt());
        o.setLabels((Map<Integer, Map<Integer, Integer>>) kryo.readObject(input, java.util.HashMap.class));
        return o;
    }
}
