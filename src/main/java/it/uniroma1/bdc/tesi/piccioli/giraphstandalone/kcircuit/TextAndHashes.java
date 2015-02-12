/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.kcircuit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author piccio
 */
public class TextAndHashes implements WritableComparable {
    
    private Text value;
    private Set<Integer> generatedHash;
    private Set<Integer> seenHash;

    public Text getId() {
        return value;
    }

    public void setId(Text id) {
        this.value = id;
    }

    public Set<Integer> getGeneratedHash() {
        return generatedHash;
    }

    public void setGeneratedHash(Set<Integer> generatedHash) {
        this.generatedHash = generatedHash;
    }

    public Set<Integer> getSeenHash() {
        return seenHash;
    }

    public void setSeenHash(Set<Integer> seenHash) {
        this.seenHash = seenHash;
    }

    public TextAndHashes() {
        this.value = new Text();
        this.generatedHash = new HashSet<Integer>();
        this.seenHash =  new HashSet<Integer>();
    }

    public TextAndHashes(Text id) {
        this.value = id;
        this.generatedHash = new HashSet<Integer>();
        this.seenHash =  new HashSet<Integer>();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        int size;
        value.write(out);
        
        this.generatedHash = new HashSet<Integer>();
        this.seenHash =  new HashSet<Integer>();
        
        size = this.generatedHash.size();
        out.writeInt(size);
        for(Integer item : generatedHash){
            out.writeInt(item);
        }
        
        size = this.seenHash.size();
        out.writeInt(size);
        for(Integer item : seenHash){
            out.writeInt(item);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size;
        value.readFields(in);
        
        size = in.readInt();
        for(int i = 0; i < size; i++){
            this.generatedHash.add(in.readInt());
        }
        size = in.readInt();
        for(int i = 0; i < size; i++){
            this.seenHash.add(in.readInt());
        }
    }

    @Override
    public int compareTo(Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
