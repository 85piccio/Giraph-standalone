/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author piccio
 */
public class DenseSubgraphVertexValue implements Writable {

    private Boolean isActive;
    private Long deletedSuperstep;

    public DenseSubgraphVertexValue() {
        isActive = Boolean.TRUE;
        deletedSuperstep = (long) 0;
    }

    public DenseSubgraphVertexValue(Boolean isActive, Long deletedSuperstep) {
        this.isActive = isActive;
        this.deletedSuperstep = deletedSuperstep;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }

    public Long getDeletedSuperstep() {
        return deletedSuperstep;
    }

    public void setDeletedSuperstep(Long deletedSuperstep) {
        this.deletedSuperstep = deletedSuperstep;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeBoolean(this.isActive);
        d.writeLong(this.deletedSuperstep);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.isActive = di.readBoolean();
        this.deletedSuperstep = di.readLong();
    }

}
