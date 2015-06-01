/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.intwritable;

import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.VertexValue;
import java.io.IOException;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 *
 * @author piccio
 */
public class VertexCompute extends BasicComputation<IntWritable, VertexValue, NullWritable, IntWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(Vertex.class);
    /**
     * Somma aggregator name
     */
    private static final String REMOVEDVERTICIES = "removedverticies";
    private static final String REMOVEDEDGES = "removededges";

//    private static final String SOGLIA = "soglia";
    private static final Double epsilon = 0.001;

    @Override
    public void compute(Vertex<IntWritable, VertexValue, NullWritable> vertex, Iterable<IntWritable> messages) throws IOException {
        if (vertex.getValue().IsActive()) {
            long superstep = this.getSuperstep();

//          TEST any case aggregato--da togliere
            if (isEven(superstep)) {//superstep = 0,2,4....

                //Calcolo soglia
                LongWritable removedEdges = this.getAggregatedValue(REMOVEDEDGES);//superstep precedente
                LongWritable removedVertex = this.getAggregatedValue(REMOVEDVERTICIES);//superstep precedente 
                Long vertices = this.getTotalNumVertices() - removedVertex.get();
                Long edges = this.getTotalNumEdges() - removedEdges.get();
                Double currDensity = (edges.doubleValue() / 2) / vertices.doubleValue();
                Double soglia = 2.0 * (1.0 + epsilon) * currDensity;
                                

                Integer removedPreviousSteps = vertex.getValue().getEdgeRemoved();
                //degree del nodo effettivi (copresi edge rimossi )
                Integer vertexDegree = vertex.getNumEdges() - removedPreviousSteps;

                if (vertexDegree <= soglia) {
                    //rimozione logica del vertice

                    aggregate(REMOVEDVERTICIES, new LongWritable(1));

                    //rimozione logica dei vertici
                    vertex.getValue().deactivate();
                    vertex.getValue().setDeletedSuperstep(superstep);

                    //rimozione logica dei Edge (solo quelli verso vertici ancora attivi, non eliminati in superstep precedenti)
                    this.sendMessageToAllEdges(vertex, vertex.getId());

                    //Sync edge rimossi con quelli eventualmente rimossi "indirettamente" step precedente
                    aggregate(REMOVEDEDGES, new LongWritable(vertexDegree));

                    vertex.voteToHalt();
                }

            } else {//superstep = 1,3,5....

                int edgeToRemove = 0;

                //rimuovo edge "di ritorno" trovati nel superstep precedente
                for (IntWritable msg : messages) {
                    edgeToRemove++;
                }

                vertex.getValue().setEdgeRemoved(vertex.getValue().getEdgeRemoved() + edgeToRemove);

                aggregate(REMOVEDEDGES, new LongWritable(edgeToRemove));
            }

        } else {
            vertex.voteToHalt();
        }
    }

    private boolean isEven(long a) {
        return (a % 2 == 0);
    }
}
