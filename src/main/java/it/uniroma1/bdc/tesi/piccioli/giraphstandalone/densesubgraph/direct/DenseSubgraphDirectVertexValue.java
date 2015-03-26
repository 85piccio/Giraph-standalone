/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct;

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
public class DenseSubgraphDirectVertexValue implements Writable {

    private Boolean isActiveInS;
    private Boolean isActiveInT;

    private Long deletedSuperstepInS;
    private Long deletedSuperstepInT;

    private Set<Long> edgeRemovedInS;
    private Set<Long> edgeRemovedInT;

    private Set<Long> IncomingEdge;

    public DenseSubgraphDirectVertexValue() {
        this.isActiveInS = Boolean.FALSE;
        this.isActiveInT = Boolean.FALSE;
        this.deletedSuperstepInS = Long.MAX_VALUE;
        this.deletedSuperstepInT = Long.MAX_VALUE;
        this.edgeRemovedInS = new HashSet<Long>();
        this.edgeRemovedInT = new HashSet<Long>();
        this.IncomingEdge = new HashSet<Long>();
    }

    public DenseSubgraphDirectVertexValue(Boolean isActiveInS, Boolean isActiveInT, Long deletedSuperstepInS, Long deletedSuperstepInT, Set<Long> edgeRemovedInS, Set<Long> edgeRemovedInT, Set<Long> IncomingEdge) {
        this.isActiveInS = isActiveInS;
        this.isActiveInT = isActiveInT;
        this.deletedSuperstepInS = deletedSuperstepInS;
        this.deletedSuperstepInT = deletedSuperstepInT;
        this.edgeRemovedInS = edgeRemovedInS;
        this.edgeRemovedInT = edgeRemovedInT;
        this.IncomingEdge = IncomingEdge;
    }

    public Boolean getIsActiveInS() {
        return isActiveInS;
    }

    public void setIsActiveInS(Boolean isActiveInS) {
        this.isActiveInS = isActiveInS;
    }

    public Boolean getIsActiveInT() {
        return isActiveInT;
    }

    public void setIsActiveInT(Boolean isActiveInT) {
        this.isActiveInT = isActiveInT;
    }

    public Long getDeletedSuperstepInS() {
        return deletedSuperstepInS;
    }

    public void setDeletedSuperstepInS(Long deletedSuperstepInS) {
        this.deletedSuperstepInS = deletedSuperstepInS;
    }

    public Long getDeletedSuperstepInT() {
        return deletedSuperstepInT;
    }

    public void setDeletedSuperstepInT(Long deletedSuperstepInT) {
        this.deletedSuperstepInT = deletedSuperstepInT;
    }

    public Set<Long> getEdgeRemovedInS() {
        return edgeRemovedInS;
    }

    public void setEdgeRemovedInS(Set<Long> edgeRemovedInS) {
        this.edgeRemovedInS = edgeRemovedInS;
    }

    public Set<Long> getEdgeRemovedInT() {
        return edgeRemovedInT;
    }

    public void setEdgeRemovedInT(Set<Long> edgeRemovedInT) {
        this.edgeRemovedInT = edgeRemovedInT;
    }

    public Set<Long> getIncomingEdge() {
        return IncomingEdge;
    }

    public void setIncomingEdge(Set<Long> IncomingEdge) {
        this.IncomingEdge = IncomingEdge;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeBoolean(this.isActiveInS);
        d.writeBoolean(this.isActiveInT);

        d.writeLong(this.deletedSuperstepInS);
        d.writeLong(this.deletedSuperstepInT);

        d.writeInt(this.edgeRemovedInS.size());
        for (Long item : this.edgeRemovedInS) {
            d.writeLong(item);
        }
        d.writeInt(this.edgeRemovedInT.size());
        for (Long item : this.edgeRemovedInT) {
            d.writeLong(item);
        }
        d.writeInt(this.IncomingEdge.size());
        for (Long item : this.IncomingEdge) {
            d.writeLong(item);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {

        this.isActiveInS = di.readBoolean();
        this.isActiveInT = di.readBoolean();
        this.deletedSuperstepInS = di.readLong();
        this.deletedSuperstepInT = di.readLong();

        int setSize = di.readInt();
        this.edgeRemovedInS = new HashSet();
        for (int i = 0; i < setSize; i++) {
            this.edgeRemovedInS.add(di.readLong());

        }
        setSize = di.readInt();
        this.edgeRemovedInT = new HashSet();
        for (int i = 0; i < setSize; i++) {
            this.edgeRemovedInT.add(di.readLong());

        }
        setSize = di.readInt();
        this.IncomingEdge = new HashSet();
        for (int i = 0; i < setSize; i++) {
            this.IncomingEdge.add(di.readLong());

        }
    }

}
