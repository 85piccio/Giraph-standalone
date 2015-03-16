/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.ksimplecycle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author piccio
 */
public class CustomMessageWithPathPlusPlus implements Writable {

    private Set<LongWritable> visitedVertex;
    private LongWritable id;
    private LongWritable value;

    public LongWritable getId() {
        return id;
    }

    public void setId(LongWritable sourceVertex) {
        this.id = sourceVertex;
    }

    public Set<LongWritable> getVisitedVertex() {
        return visitedVertex;
    }

    public void setVisitedVertex(Set<LongWritable> visitedVertex) {
        this.visitedVertex = visitedVertex;
    }

    public LongWritable getValue() {
        return value;
    }

    public void setValue(LongWritable degreeOfVertex) {
        this.value = degreeOfVertex;
    }

    public CustomMessageWithPathPlusPlus() {
        this.visitedVertex = new HashSet<LongWritable>();
        this.id = new LongWritable();
        this.value = new LongWritable();
    }

    public CustomMessageWithPathPlusPlus(HashSet<LongWritable> path, LongWritable tx, LongWritable tz) {

        this.visitedVertex = path;
        this.id = tx;
        this.value = tz;
    }

    public CustomMessageWithPathPlusPlus(LongWritable tx, LongWritable tz) {

        this.visitedVertex = new HashSet<LongWritable>();
        this.id = tx;
        this.value = tz;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(visitedVertex.size());
        for (LongWritable item : visitedVertex) {
            out.writeLong(item.get());
        }
        out.writeLong(this.id.get());
        out.writeLong(this.value.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        int size = in.readInt();
        this.visitedVertex = new HashSet<LongWritable>();
        for (int i = 0; i < size; i++) {
            this.visitedVertex.add(new LongWritable(in.readLong()));
        }
        this.id = new LongWritable(in.readLong());
        this.value = new LongWritable(in.readLong());
    }

}
