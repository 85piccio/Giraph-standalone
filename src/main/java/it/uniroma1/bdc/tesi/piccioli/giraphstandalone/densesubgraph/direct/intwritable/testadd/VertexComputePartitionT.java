package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct.intwritable.testadd;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.python.google.common.collect.Iterables;

/**
 *
 * @author piccio
 *
 * Classe vertici partizione T
 */
public class VertexComputePartitionT extends BasicComputation<IntWritable, VertexValue, IntWritable, IntWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(VertexComputePartitionT.class);

    /**
     * Somma aggregator name
     */
    private static final String REMOVEDVERTICIESINS = "removedVerticiesFromS";
    private static final String REMOVEDVERTICIESINT = "removedVerticiesFromT";
    private static final String REMOVEDEDGES = "removedEdges";

    private static final Double epsilon = 0.001;

    @Override
    public void compute(Vertex<IntWritable, VertexValue, IntWritable> vertex, Iterable<IntWritable> messages) throws IOException {
        Long superstep = this.getSuperstep();
//	System.out.println("T");

        //check vertex status
        if (!vertex.getValue().getPartitionS().IsActive() && !vertex.getValue().getPartitionT().IsActive()) {
            vertex.voteToHalt();
        }

        if (superstep > 2) {
            if (this.isEven(superstep)) {
                //2, 4, 6 ..
//
                if (vertex.getValue().getPartitionT().IsActive()) {

                    //CALCOLO SOGLIA S
                    LongWritable removedVertexInT = this.getAggregatedValue(REMOVEDVERTICIESINT);//superstep precedente	
                    LongWritable removedEdges = this.getAggregatedValue(REMOVEDEDGES);//superstep precedente

                    Long verticesInT = this.getTotalNumVertices() - removedVertexInT.get();
                    //EpSpSPp --> |E(S, T )| = |E ∩ (S×T)|
                    Long totEdge = this.getTotalNumEdges() / 2;
                    Long EpSTp = totEdge - removedEdges.get();

                    // soglia = (1 + epsilon) * (|E(S, T)| / |S| )
                    Double soglia = (1 + epsilon) * ((EpSTp.doubleValue()) / verticesInT.doubleValue());

                    int inDegree = vertex.getValue().getInDegree().get() - vertex.getValue().getPartitionT().getEdgeRemoved();
                    if (inDegree <= soglia) {
                        //elimino vertice dalla partizione T
                        vertex.getValue().getPartitionT().deactivate();
                        vertex.getValue().getPartitionT().setDeletedSuperstep(superstep);

                        this.aggregate(REMOVEDVERTICIESINT, new LongWritable(1));
//                        this.aggregate(REMOVEDEDGES, new LongWritable(inDegree));

                        //invio messaggi a vertici in Partizione S che diminueranno il loro inDegree
                        for (Edge<IntWritable, IntWritable> edge : vertex.getEdges()) {
                            if (edge.getValue().get() == 1) {
                                this.sendMessage(edge.getTargetVertexId(), vertex.getId());
                            }
                        }
                    }
                }
            } else {
                //3,5,7 ..
                //vertici nella partizione S
                if (vertex.getValue().getPartitionS().IsActive()) {

                    int edgeToRemove = Iterables.size(messages);
                    this.aggregate(REMOVEDEDGES, new LongWritable(edgeToRemove));

                    //aggiorno tot vertici rimossi da partizione
                    int totRemoved = vertex.getValue().getPartitionS().getEdgeRemoved() + edgeToRemove;
                    vertex.getValue().getPartitionS().setEdgeRemoved(totRemoved);

                    //Caso vertice rimane senza edge uscenti -> da eliminare
                    if (totRemoved == vertex.getValue().getOutDegree().get()) {
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
