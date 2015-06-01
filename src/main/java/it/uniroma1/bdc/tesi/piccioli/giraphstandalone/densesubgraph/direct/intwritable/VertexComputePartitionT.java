package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct.intwritable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.python.google.common.collect.Iterables;

/**
 *
 * @author piccio
 *
 * Classe vertici partizione T
 */
public class VertexComputePartitionT extends BasicComputation<IntWritable, VertexValue, NullWritable, IntWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(VertexComputePartitionT.class);

    /**
     * Somma aggregator name
     */
    private static final String REMOVEDEDGES = "removedEdges";
    private static final String REMOVEDVERTICIESINT = "removedVerticiesFromT";
    private static final String REMOVEDVERTICIESINS = "removedVerticiesFromS";

    private static final Double epsilon = 0.001;

    @Override
    public void compute(Vertex<IntWritable, VertexValue, NullWritable> vertex, Iterable<IntWritable> messages) throws IOException {
        Long superstep = this.getSuperstep();
//	System.out.println("T");

        //check vertex status
        if (!vertex.getValue().getPartitionS().IsActive() && !vertex.getValue().getPartitionT().IsActive()) {
            vertex.voteToHalt();
        }

        if (superstep > 1) {
            if (this.isEven(superstep)) {
                //2, 4, 6 ..
//
                if (vertex.getValue().getPartitionT().IsActive()) {

                    Double soglia = 0.0;
                    if (superstep > 2) {
                        //CALCOLO SOGLIA T
                        LongWritable removedVertexInT = this.getAggregatedValue(REMOVEDVERTICIESINT);//superstep precedente	
                        LongWritable removedEdges = this.getAggregatedValue(REMOVEDEDGES);//superstep precedente

                        Long verticesInT = this.getTotalNumVertices() - removedVertexInT.get();
                        //EpSpSPp --> |E(S, T )| = |E ∩ (S×T)|
                        Long EpSTp = this.getTotalNumEdges() - removedEdges.get();

                        // soglia = (1 + epsilon) * (|E(S, T)| / |T| )
                        soglia = (1 + epsilon) * ((EpSTp.doubleValue()) / verticesInT.doubleValue());
                    }

                    int inDegree = vertex.getValue().getIncomingEdge().size();
                    if (inDegree <= soglia) {
                        //rimuovo vertice da partizione T
                        vertex.getValue().getPartitionT().deactivate();
                        vertex.getValue().getPartitionT().setDeletedSuperstep(superstep);
                        this.aggregate(REMOVEDVERTICIESINT, new LongWritable(1));

                        for (Integer inEdge : vertex.getValue().getIncomingEdge()) {
                            this.sendMessage(new IntWritable(inEdge), vertex.getId());
                        }
                        vertex.getValue().getIncomingEdge().clear();
                    }
                }
            } else {
                //3,5,7 ..
                if (vertex.getValue().getPartitionS().IsActive()) {

                    int edgeToRemove = Iterables.size(messages);
                    this.aggregate(REMOVEDEDGES, new LongWritable(edgeToRemove));

                    //aggiorno outDegree S-->T
                    int edgeYetRemoved = vertex.getValue().getPartitionS().getEdgeRemoved();
                    vertex.getValue().getPartitionS().setEdgeRemoved(edgeYetRemoved + edgeToRemove);

                    //Caso vertice rimane senza edge uscenti -> da eliminare
                    if ((edgeYetRemoved + edgeToRemove) == vertex.getNumEdges()) {
                        vertex.getValue().getPartitionS().deactivate();
                        vertex.getValue().getPartitionS().setDeletedSuperstep(superstep - 1);
                        this.aggregate(REMOVEDVERTICIESINS, new LongWritable(1));
                    }

                }
            }
        }
    }

    private boolean isEven(long a) {
        return (a % 2 == 0);
    }
}
