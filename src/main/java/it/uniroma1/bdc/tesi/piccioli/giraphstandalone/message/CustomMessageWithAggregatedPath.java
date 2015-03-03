/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author piccio
 */
public class CustomMessageWithAggregatedPath implements Writable {

    private ArrayList<HashSet<Text>> visitedVertex;

    public ArrayList<HashSet<Text>> getVisitedVertex() {
        return visitedVertex;
    }

    public void setVisitedVertex(ArrayList<HashSet<Text>> visitedVertex) {
        this.visitedVertex = visitedVertex;
    }



    public CustomMessageWithAggregatedPath() {
        visitedVertex = new ArrayList<HashSet<Text>>();
    }
    
    

    public CustomMessageWithAggregatedPath(ArrayList<HashSet<Text>> visitedVertex) {
        this.visitedVertex = visitedVertex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(visitedVertex.size());
        for (Set<Text> item : visitedVertex) {
            out.writeInt(item.size());
            for (Text itemText : item) {
                itemText.write(out);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();

        visitedVertex = new ArrayList<HashSet<Text>>(size);
        for (int i = 0; i < size; i++) {
            int sizeItem = in.readInt();
            visitedVertex.set(i,new HashSet<Text>());
            for (int j = 0; j < sizeItem; j++) {
                Text toAdd = new Text();
                toAdd.readFields(in);
                visitedVertex.get(i).add(toAdd);
            }
        }


    }

}
