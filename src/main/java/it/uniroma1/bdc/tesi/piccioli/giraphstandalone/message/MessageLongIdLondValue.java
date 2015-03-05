/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author piccio
 */
public class MessageLongIdLondValue implements Writable {

    private LongWritable id;
    private LongWritable value;

    public MessageLongIdLondValue(LongWritable id, LongWritable value) {
        this.id = id;
        this.value = value;
    }

    public MessageLongIdLondValue() {
    }

    public LongWritable getId() {
        return id;
    }

    public void setId(LongWritable id) {
        this.id = id;
    }

    public LongWritable getValue() {
        return value;
    }

    public void setValue(LongWritable value) {
        this.value = value;
    }


    @Override
    public void write(DataOutput d) throws IOException {
        d.writeLong(this.id.get());
        d.writeLong(this.value.get());
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.id = new LongWritable(di.readLong());
        this.value = new LongWritable(di.readLong());
    }
}
