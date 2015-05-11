/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct.intwritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author piccio
 */
public class VertexValue implements Writable {

    private it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.longwritable.VertexValue partitionS;
    private it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.longwritable.VertexValue partitionT;

    private Set<Integer> IncomingEdge;

    public VertexValue() {
	this.IncomingEdge = new HashSet();
	this.partitionS = new it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.longwritable.VertexValue();
	this.partitionT = new it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.longwritable.VertexValue();
    }

    public it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.longwritable.VertexValue getPartitionS() {
	return partitionS;
    }

    public void setPartitionS(it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.longwritable.VertexValue partitionS) {
	this.partitionS = partitionS;
    }

    public it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.longwritable.VertexValue getPartitionT() {
	return partitionT;
    }

    public void setPartitionT(it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.longwritable.VertexValue partitionT) {
	this.partitionT = partitionT;
    }

    public Set<Integer> getIncomingEdge() {
	return IncomingEdge;
    }

    public void setIncomingEdge(Set<Integer> IncomingEdge) {
	this.IncomingEdge = IncomingEdge;
    }

    @Override
    public void write(DataOutput d) throws IOException {
	this.partitionS.write(d);
	this.partitionT.write(d);
	d.writeInt(this.IncomingEdge.size());
	for (Integer e : this.IncomingEdge) {
	    d.writeInt(e);
	}
    }

    @Override
    public void readFields(DataInput di) throws IOException {
	this.partitionS.readFields(di);
	this.partitionT.readFields(di);

	int size = di.readInt();
	this.IncomingEdge = new HashSet<>();
	for (int i = 0; i < size; i++) {
	    this.IncomingEdge.add(di.readInt());
	}
    }

    @Override
    public String toString() {
	return "DenseSubgraphDirectVertexValue{" + "partitionS=" + partitionS + ", partitionT=" + partitionT + ", IncomingEdge=" + IncomingEdge + '}';
    }

}
