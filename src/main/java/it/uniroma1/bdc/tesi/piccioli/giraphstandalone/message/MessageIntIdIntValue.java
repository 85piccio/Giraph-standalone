/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author piccio
 */
public class MessageIntIdIntValue implements Writable{

    private IntWritable id;
    private IntWritable value;

    public MessageIntIdIntValue(IntWritable id, IntWritable value) {
        this.id = id;
        this.value = value;
    }

    public MessageIntIdIntValue() {
    }

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public IntWritable getValue() {
        return value;
    }

    public void setValue(IntWritable value) {
        this.value = value;
    }


    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(this.id.get());
        d.writeInt(this.value.get());
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.id = new IntWritable(di.readInt());
        this.value = new IntWritable(di.readInt());
    }
    
}
