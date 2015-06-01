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

/**
 *
 * @author piccio
 *
 * Vertex in partition S Classe che viene eseguita anche in fase di init
 * (creazione incoming edge nei primi 2 supertep)
 */
public class VertexComputePartitionS extends BasicComputation<IntWritable, VertexValue, NullWritable, IntWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(VertexComputePartitionS.class);

    /**
     * Somma aggregator name
     */
    private static final String REMOVEDVERTICIESINS = "removedVerticiesFromS";
    private static final String REMOVEDVERTICIESINT = "removedVerticiesFromT";
    private static final String REMOVEDEDGES = "removedEdges";

    private static final Double epsilon = 0.001;

    @Override
    public void compute(Vertex<IntWritable, VertexValue, NullWritable> vertex, Iterable<IntWritable> messages) throws IOException {
        Long superstep = this.getSuperstep();

        //check vertex status
        if (!vertex.getValue().getPartitionS().IsActive() && !vertex.getValue().getPartitionT().IsActive()) {
            vertex.voteToHalt();
        }

        if (superstep > 1) {

            //Partition S
            if (this.isEven(superstep)) {
                //2, 4, 6 ..
                if (vertex.getValue().getPartitionS().IsActive()) {
                    Double soglia = 0.0;
                    if (superstep > 2) {
                        //CALCOLO SOGLIA S
                        LongWritable removedVertexInS = this.getAggregatedValue(REMOVEDVERTICIESINS);//superstep precedente	
                        LongWritable removedEdges = this.getAggregatedValue(REMOVEDEDGES);//superstep precedente

                        Long verticesInS = this.getTotalNumVertices() - removedVertexInS.get();
                        //EpSpSPp --> |E(S, T )| = |E ∩ (S×T)|
                        Long EpSTp = this.getTotalNumEdges() - removedEdges.get();

                        // soglia = (1 + epsilon) * (|E(S, T)| / |S| )
                        soglia = (1 + epsilon) * ((EpSTp.doubleValue()) / verticesInS.doubleValue());
                    }

                    int outDegree = vertex.getNumEdges() - vertex.getValue().getPartitionS().getEdgeRemoved();
                    if (outDegree <= soglia) {
                        //elimino vertice dalla partizione S
                        vertex.getValue().getPartitionS().deactivate();
                        vertex.getValue().getPartitionS().setDeletedSuperstep(superstep);

                        this.aggregate(REMOVEDVERTICIESINS, new LongWritable(1));

                        //invio messaggi a vertici in Partizione T che diminueranno il loro inDegree
                        this.sendMessageToAllEdges(vertex, vertex.getId());
                        this.aggregate(REMOVEDEDGES, new LongWritable(outDegree));
                    }
                }
            } else {
		//3,5,7 ..
                //vertici nella partizione T

                //elimino da lista incoming edge
                if (vertex.getValue().getPartitionT().IsActive()) {
                    for (IntWritable msg : messages) {
                        vertex.getValue().getIncomingEdge().remove(msg.get());
                    }

                    //caso vertice rimane senza nodi entranti --> da eliminare
                    if (vertex.getValue().getIncomingEdge().isEmpty()) {
                        vertex.getValue().getPartitionT().deactivate();
                        vertex.getValue().getPartitionT().setDeletedSuperstep(superstep - 1);
                        this.aggregate(REMOVEDVERTICIESINT, new LongWritable(1));
                    }

                }
            }

        } else {
            //INIT - Superstep 0 e 1 creano lista incoming edge 
            if (superstep == 0) {
                this.sendMessageToAllEdges(vertex, vertex.getId());
            }
            if (superstep == 1) {
                for (IntWritable msg : messages) {
                    vertex.getValue().getIncomingEdge().add(msg.get());
                }
            }
        }

    }

    private boolean isEven(long a) {
        return (a % 2 == 0);
    }
}
