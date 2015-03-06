/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable {

    private Text vertexId;
    private Text vertexDegree;

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public Text getFirst() {
        return vertexId;
    }

    public Text getSecond() {
        return vertexDegree;
    }

    public final void set(Text first, Text second) {
        this.vertexId = first;
        this.vertexDegree = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vertexId.readFields(in);
        vertexDegree.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vertexId.write(out);
        vertexDegree.write(out);
    }

    @Override
    public String toString() {
        return vertexId + " " + vertexDegree;
    }

//    @Override
//    public int compareTo(TextPair tp) {
//        int cmp = vertexId.compareTo(tp.vertexId);
//
//        if (cmp != 0) {
//            return cmp;
//        }
//
//        return vertexDegree.compareTo(tp.vertexDegree);
//    }
    @Override
    public int hashCode() {
        return vertexId.hashCode() * 163 + vertexDegree.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return vertexId.equals(tp.vertexId) && vertexDegree.equals(tp.vertexDegree);
        }
        return false;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return vertexId.compareTo(tp.vertexId);
        }
        return Integer.MIN_VALUE;//error

    }

}
