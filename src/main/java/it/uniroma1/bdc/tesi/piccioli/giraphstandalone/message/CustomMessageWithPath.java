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

   
    private Set<Text> visitedVertex;
    private Text sourceVertex;

    public Text getSourceVertex() {
        return sourceVertex;
    }

    public void setSourceVertex(Text sourceVertex) {
        this.sourceVertex = sourceVertex;
    }
    
    public Set<Text> getVisitedVertex() {
        return visitedVertex;
    }

    public void setVisitedVertex(Set<Text> visitedVertex) {
        this.visitedVertex = visitedVertex;
    }

    public CustomMessageWithPath() {
        this.visitedVertex = new HashSet<Text>();
        this.sourceVertex = new Text();
    }

    public CustomMessageWithPath(HashSet<Text> path, Text tx) {
    
        this.visitedVertex = path;
        this.sourceVertex = tx;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(visitedVertex.size());
        for (Text item : visitedVertex) {
            item.write(out);
        }
        sourceVertex.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
        int size = in.readInt();
        visitedVertex = new HashSet<Text>();
        for (int i = 0; i < size; i++) {
            Text toAdd = new Text();
            toAdd.readFields(in);
            visitedVertex.add(toAdd);
        }
        sourceVertex.readFields(in); 
    }

}
