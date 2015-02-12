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
public class CustomMessage implements Writable {

    private Text source;
    private int message;

    public CustomMessage() {
        this.source = new Text();
    }

    public CustomMessage(Text id, int message) {
        this.source = id;
        this.message = message;
    }

    public Text getSource() {
        return source;
    }

    public void setSource(Text id) {
        this.source = id;
    }

    public int getMessage() {
        return message;
    }

    public void setMessage(int message) {
        this.message = message;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        source.write(d);
        d.writeInt(message);

    }

    @Override
    public void readFields(DataInput di) throws IOException {
        source.readFields(di);
        message = di.readInt();
    }

}
