/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author piccio
 */
public class CustomMessageWithPath implements Writable {

   
    private Set<Text> path;
    private Text t;

    public Text getT() {
        return t;
    }

    public void setT(Text t) {
        this.t = t;
    }


    
    public Set<Text> getPath() {
        return path;
    }

    public void setPath(Set<Text> path) {
        this.path = path;
    }

    public CustomMessageWithPath() {
        this.path = new HashSet<Text>();
        this.t = new Text();
    }

    public CustomMessageWithPath(HashSet<Text> path, Text tx) {
    
        this.path = path;
        this.t = tx;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(path.size());
        for (Text item : path) {
            item.write(out);
        }
        t.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
        int size = in.readInt();
        path = new HashSet<Text>();
        for (int i = 0; i < size; i++) {
            Text toAdd = new Text();
            toAdd.readFields(in);
            path.add(toAdd);
        }
        t.readFields(in); 
    }

}
