/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph;

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
public class DenseSubgraphVertexValue implements Writable {

    private Boolean isActive;
    private Long deletedSuperstep;
    private Set<Long> edgeRemoved;

    public DenseSubgraphVertexValue() {
        isActive = Boolean.TRUE;
        deletedSuperstep = Long.MAX_VALUE;
        edgeRemoved = new HashSet<Long>();
    }

    public DenseSubgraphVertexValue(Boolean isActive, Long deletedSuperstep, Set set) {
        this.isActive = isActive;
        this.deletedSuperstep = deletedSuperstep;
        this.edgeRemoved = set;
    }

    public Boolean IsActive() {
        return isActive;
    }

    public void deactive() {
        this.isActive = Boolean.FALSE;
    }

    public Long getDeletedSuperstep() {
        return deletedSuperstep;
    }

    public void setDeletedSuperstep(Long deletedSuperstep) {
        this.deletedSuperstep = deletedSuperstep;
    }

    public Set<Long> getEdgeRemoved() {
        return edgeRemoved;
    }

    public void setEdgeRemoved(Set<Long> edgeRemoved) {
        this.edgeRemoved = edgeRemoved;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeBoolean(this.isActive);
        d.writeLong(this.deletedSuperstep);
        d.writeInt(this.edgeRemoved.size());
        for(Long item  : this.edgeRemoved){
            d.writeLong(item);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.isActive = di.readBoolean();
        this.deletedSuperstep = di.readLong();
       
        int setSize = di.readInt();
        this.edgeRemoved = new HashSet();
        for (int i = 0; i< setSize; i++ ){
            this.edgeRemoved.add(di.readLong());
            
        }
    }

}
