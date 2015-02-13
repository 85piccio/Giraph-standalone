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
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author piccio
 */
public class TextValueAndSetPerSuperstep implements WritableComparable {

    private Text value;
//    private Map<Text, Set<Text>> setPerSuperstep;
    private Map<LongWritable, DoubleWritable> setPerSuperstep;

    public Text getValue() {
        return value;
    }

    public void setValue(Text id) {
        this.value = id;
    }

//    public Map<Text, Set<Text>> getSetPerSuperstep() {
//        return setPerSuperstep;
//    }
//
//    public void setSetPerSuperstep(Map<Text, Set<Text>> setPerSuperstep) {
//        this.setPerSuperstep = setPerSuperstep;
//    }

    public Map<LongWritable, DoubleWritable> getSetPerSuperstep() {
        return setPerSuperstep;
    }

    public void setSetPerSuperstep(Map<LongWritable, DoubleWritable> setPerSuperstep) {
        this.setPerSuperstep = setPerSuperstep;
    }
    

    public TextValueAndSetPerSuperstep() {
        this.value = new Text();
//        this.setPerSuperstep = new HashMap<Text, Set<Text>>();
        this.setPerSuperstep = new HashMap<LongWritable, DoubleWritable>();
    }

    public TextValueAndSetPerSuperstep(Text id) {
        this.value = id;
//        this.setPerSuperstep = new HashMap<Text, Set<Text>>();
        this.setPerSuperstep = new HashMap<LongWritable, DoubleWritable>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int size;
        int sizeSet;
        value.write(out);
        

        size = this.setPerSuperstep.size();
        out.writeInt(size);//scrivo numero di chiavi nella MAP
        for (LongWritable itemKey : setPerSuperstep.keySet()) {
            itemKey.write(out);//Scrivo chiave 
            this.setPerSuperstep.get(itemKey).write(out);//scrivo val
//
//            sizeSet = this.setPerSuperstep.get(itemKey).size();//Numero elementi per la chiave
//            out.writeInt(sizeSet);//Scrivo numero elementi 
//            
//            for (Text item : setPerSuperstep.get(itemKey)) {
//                item.write(out);//Scrivo elementi
//            }
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size;
        value.readFields(in);

        
        size = in.readInt();//Leggo numero di key da inserire nella MAP
        for (int i = 0; i < size; i++) {

            LongWritable key = new LongWritable();
            key.readFields(in);//Leggo Chiave
            DoubleWritable valueh = new DoubleWritable();
            value.readFields(in);//Leggo Chiave
            this.setPerSuperstep.put(key, valueh);//inserisco chiave nella MAP

//            int sizeSet = in.readInt();//Leggo numero elementi nel set puntanto dal value in setPerSuperstep
//
//            for (int j = 0; j < sizeSet; j++) {
//                Text toAdd = new Text();
//                toAdd.readFields(in);
//                this.setPerSuperstep.get(key).add(toAdd);
//            }

        }

//      
    }

    @Override
    public int compareTo(Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
