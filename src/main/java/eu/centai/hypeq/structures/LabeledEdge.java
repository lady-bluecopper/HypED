package eu.centai.hypeq.structures;

import java.io.Serializable;
import java.util.Objects;

public class LabeledEdge implements Serializable {

    private int src;
    private int dst;
    private int label;
    
    public LabeledEdge(int src, int dst, int label) {
        this.src = src;
        this.dst = dst;
        this.label = label;
    }

    @Override
    public String toString() {
        return "[" + src + "-" + dst + ", " + label + "]";
    }
    
    public String toSimpleString() {
        return src + "\t" + dst + "\t " + label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LabeledEdge edge = (LabeledEdge) o;
        return ((src == edge.src && dst == edge.dst) ||
                (src == edge.dst && dst == edge.src)) &&
                label == edge.label;
    }

    @Override
    public int hashCode() {
        return Objects.hash(src, dst, label);
    }

    public int getSrc() {
        return src;
    }

    public int getDst() {
        return dst;
    }

    public int getLabel() {
        return label;
    }
}
