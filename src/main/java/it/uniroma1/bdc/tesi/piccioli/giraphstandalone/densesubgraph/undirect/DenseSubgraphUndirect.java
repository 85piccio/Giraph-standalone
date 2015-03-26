/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect;

import java.io.IOException;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 *
 * @author piccio
 */
public class DenseSubgraphUndirect extends BasicComputation<LongWritable, DenseSubgraphUndirectVertexValue, NullWritable, LongWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(DenseSubgraphUndirect.class);
    /**
     * Somma aggregator name
     */
    private static final String REMOVEDVERTICIES = "removedVerticies";
    private static final String REMOVEDEDGES = "removedEdges";

    private static final String SOGLIA = "soglia";

    @Override
    public void compute(Vertex<LongWritable, DenseSubgraphUndirectVertexValue, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {
        if (vertex.getValue().IsActive()) {
            long superstep = this.getSuperstep();

            Integer removedPreviousSteps = vertex.getValue().getEdgeRemoved();

            if (isEven(superstep)) {//superstep = 0,2,4....

                Double soglia = this.getContext().getConfiguration().getDouble(SOGLIA, 0.0);

                //degree del nodo effettivi (copresi edge rimossi )
                Integer vertexDegree = vertex.getNumEdges() - removedPreviousSteps;

                if (vertexDegree <= soglia) {
                    //rimozione logica del vertice

                    this.aggregate(REMOVEDVERTICIES, new LongWritable(1));

                    //rimozione logica dei vertici
                    vertex.getValue().deactive();
                    vertex.getValue().setDeletedSuperstep(superstep);

                    //rimozione logica dei Edge (solo quelli verso vertici ancora attivi, non eliminati in superstep precedenti)
                    this.sendMessageToAllEdges(vertex, vertex.getId());

                    //Sync edge rimossi con quelli eventualmente rimossi "indirettamente" step precedente
                    this.aggregate(REMOVEDEDGES, new LongWritable(vertexDegree));

                    vertex.voteToHalt();
                }

            } else {//superstep = 1,3,5....

                int edgeToRemove = 0;

                //rimuovo edge "di ritorno" trovati nel superstep precedente
                for (LongWritable msg : messages) {
                    edgeToRemove++;
                }

                vertex.getValue().setEdgeRemoved(vertex.getValue().getEdgeRemoved() + edgeToRemove);

                this.aggregate(REMOVEDEDGES, new LongWritable(edgeToRemove));
            }

        } else {
            vertex.voteToHalt();
        }
    }

    private boolean isEven(long a) {
        return (a % 2 == 0);
    }
}
