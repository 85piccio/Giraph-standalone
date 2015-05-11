/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.tools.direct;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 *
 * @author piccio
 */
public class IntegrityCheck  extends BasicComputation<IntWritable, DoubleWritable, NullWritable, IntWritable>  {

    @Override
    public void compute(Vertex<IntWritable, DoubleWritable, NullWritable> vertex, Iterable<IntWritable> messagges) throws IOException {
          long superstep = this.getSuperstep();
        Iterable<Edge<IntWritable, NullWritable>> edges = vertex.getEdges();

        if (superstep == 0) {
            //invio messaggi per controllo esistenza arco inverso
//            this.sendMessageToAllEdges(vertex, vertex.getId());

            //controllo unicità edge
            Set<Integer> edgeSet = new HashSet<>();
            for (Edge<IntWritable, NullWritable> edge : edges) {
                if (!edgeSet.contains(edge.getTargetVertexId().get())) {
                    edgeSet.add(edge.getTargetVertexId().get());
                } else {
                    //Segnalo errore edge doppio
                    System.out.println(vertex.getId() + "-->" + edge.getTargetVertexId() + " doppio");
                }
                
                //controllo se sono presenti archi verso su stesso vertice
                if(edge.getTargetVertexId().get() == vertex.getId().get()){
                    System.out.println("su vertice " + vertex.getId() + " presente arco su se stesso");
                }
            }            
            vertex.voteToHalt();

        }
    }
    
}
