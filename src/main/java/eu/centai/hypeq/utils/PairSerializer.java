package eu.centai.hypeq.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.javatuples.Pair;

class PairSerializer extends Serializer<Pair<Integer, Integer>> {

    @Override
    public void write(Kryo kryo, Output output, Pair<Integer, Integer> t) {
        output.writeInt(t.getValue0());
        output.writeInt(t.getValue1());
    }

    @Override
    public Pair<Integer, Integer> read(Kryo kryo, Input input, Class<? extends Pair<Integer, Integer>> type) {
        int f = input.readInt();
        int s = input.readInt();
        return new Pair(f, s);
    }

}
