/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author piccio
 */
public class MessageTextIdTextValue implements Writable {

    private Text id;
    private Text value;

    public MessageTextIdTextValue(Text id, Text value) {
        this.id = id;
        this.value = value;
    }

    public MessageTextIdTextValue() {
        this.id = new Text();
        this.value = new Text();
    }

    public Text getId() {
        return id;
    }

    public void setId(Text id) {
        this.id = id;
    }

    public Text getValue() {
        return value;
    }

    public void setValue(Text value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        this.id.write(d);
        this.value.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.id.readFields(di);
        this.value.readFields(di);
    }

}
