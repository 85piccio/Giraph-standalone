/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 *
 * @author piccio
 */
public class DenseSubgraphUndirect extends BasicComputation<LongWritable, DenseSubgraphVertexValue, NullWritable, LongWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(DenseSubgraphUndirect.class);
    /**
     * Somma aggregator name
     */
//    private static final String VERTECIES = "vertecies";
//    private static final String EDGES = "edges";

    private static final String REMOVEDVERTICIES = "removedVerticies";
    private static final String REMOVEDEDGES = "removedEdges";
    private static final String SOGLIA = "soglia";

    @Override
    public void compute(Vertex<LongWritable, DenseSubgraphVertexValue, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {
        long superstep = this.getSuperstep();

        int edgeToRemove = 0;

        if (isEven(superstep)) {//superstep = 0,2,4....

            Double soglia = this.getContext().getConfiguration().getDouble(SOGLIA, 0.0);

            //degree del nodo effettivi (copresi edge rimossi )
            Integer degree = vertex.getNumEdges() - vertex.getValue().getEdgeRemoved().size();

            if (degree <= soglia) {
                //rimozione logica del vertice
                if (vertex.getValue().IsActive()) {
                    this.aggregate(REMOVEDVERTICIES, new LongWritable(1));

                    //rimozione logica dei vertici
                    vertex.getValue().deactive();
                    vertex.getValue().setDeletedSuperstep(superstep);

                    //rimozione logica dei Edge (solo quelli verso vertici ancora attivi, non eliminati in superstep precedenti)
                    for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                        //se edge non è già stato rimosso
                        if (!vertex.getValue().getEdgeRemoved().contains(edge.getTargetVertexId().get())) {
                            //mando messaggio a nodi vicini di considerare l'edge rimosso (rimuovere l'altra direzione)
                            this.sendMessageToAllEdges(vertex, vertex.getId());
                            vertex.getValue().getEdgeRemoved().add(edge.getTargetVertexId().get());
                            //Rimuovo 
                            edgeToRemove++;
                        }
                    }
                }
                vertex.voteToHalt();
            }

        } else {//superstep = 1,3,5....

            //rimuovo edge "di ritorno" trovati nel superstep precedente
            for (LongWritable msg : messages) {
                //se edge non è già stato rimosso
                if (!vertex.getValue().getEdgeRemoved().contains(msg.get())) {
                    vertex.getValue().getEdgeRemoved().add(msg.get());
                    edgeToRemove++;
                }
            }

        }

        if (edgeToRemove > 0) {
            this.aggregate(REMOVEDEDGES, new LongWritable(edgeToRemove));
        }
    }

    private boolean isEven(long a) {
        return (a % 2 == 0);
    }
}
