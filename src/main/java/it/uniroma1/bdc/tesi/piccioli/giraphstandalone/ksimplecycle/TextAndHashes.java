/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.ksimplecycle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
    private Map<Text, Set<Integer>> seenHash;

    public Text getValue() {
        return value;
    }

    public void setValue(Text id) {
        this.value = id;
    }

    public Set<Integer> getGeneratedHash() {
        return generatedHash;
    }

    public void setGeneratedHash(Set<Integer> generatedHash) {
        this.generatedHash = generatedHash;
    }

    public Map<Text, Set<Integer>> getSeenHash() {
        return seenHash;
    }

    public void setSeenHash(Map<Text, Set<Integer>> seenHash) {
        this.seenHash = seenHash;
    }

    public TextAndHashes() {
        this.value = new Text();
        this.generatedHash = new HashSet<Integer>();
        this.seenHash = new HashMap<Text, Set<Integer>>();
    }

    public TextAndHashes(Text id) {
        this.value = id;
        this.generatedHash = new HashSet<Integer>();
        this.seenHash = new HashMap<Text, Set<Integer>>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int size;
        int sizeSet;
        value.write(out);

        this.generatedHash = new HashSet<Integer>();
        this.seenHash = new HashMap<Text, Set<Integer>>();

        size = this.generatedHash.size();
        out.writeInt(size);
        for (Integer item : generatedHash) {
            out.writeInt(item);
        }

        size = this.seenHash.size();
        out.writeInt(size);//scrivo numero di chiavi nella MAP
        for (Text itemKey : seenHash.keySet()) {
            itemKey.write(out);//Scrivo chiave 

            sizeSet = this.seenHash.get(itemKey).size();//Numero elementi per la chiave
            out.writeInt(sizeSet);//Scrivo numero elementi 
            
            for (Integer item : seenHash.get(itemKey)) {
                out.writeInt(item);//Scrivo elementi
            }
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size;
        value.readFields(in);

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            this.generatedHash.add(in.readInt());
        }

        size = in.readInt();//Leggo numero di key da inserire nella MAP
        for (int i = 0; i < size; i++) {

            Text key = new Text();
            key.readFields(in);//Leggo Chiave
            this.seenHash.put(key, new HashSet());//inserisco chiave nella MAP

            int sizeSet = in.readInt();//Leggo numero elementi nel set puntanto dal value in seenHash

            for (int j = 0; j < sizeSet; j++) {
                this.seenHash.get(key).add(in.readInt());
            }

        }

//      
    }

    @Override
    public int compareTo(Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
