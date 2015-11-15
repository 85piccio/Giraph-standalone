/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct.intwritable.testadd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author piccio
 */
public class VertexValue implements Writable {

    private it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.VertexValue partitionS;//outDegree
    private it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.VertexValue partitionT;//inDegree
    private IntWritable inDegree;
    private IntWritable outDegree;

    public IntWritable getOutDegree() {
        return outDegree;
    }

    public void setOutDegree(IntWritable outDegree) {
        this.outDegree = outDegree;
    }

    public VertexValue() {

        this.partitionS = new it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.VertexValue();
        this.partitionT = new it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.VertexValue();
        inDegree = new IntWritable(0);
        outDegree = new IntWritable(0);
    }

    public IntWritable getInDegree() {
        return inDegree;
    }

    public void setInDegree(IntWritable inDegree) {
        this.inDegree = inDegree;
    }

    public it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.VertexValue getPartitionS() {
        return partitionS;
    }

    public void setPartitionS(it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.VertexValue partitionS) {
        this.partitionS = partitionS;
    }

    public it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.VertexValue getPartitionT() {
        return partitionT;
    }

    public void setPartitionT(it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.VertexValue partitionT) {
        this.partitionT = partitionT;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        this.partitionS.write(d);
        this.partitionT.write(d);
        this.inDegree.write(d);
        this.outDegree.write(d);

    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.partitionS.readFields(di);
        this.partitionT.readFields(di);
        this.inDegree.readFields(di);
        this.outDegree.readFields(di);

    }

    @Override
    public String toString() {
        return "VertexValue{" + "partitionS=" + partitionS + ", partitionT=" + partitionT + ", inDegree=" + inDegree + ", outDegree=" + outDegree + '}';
    }

}
