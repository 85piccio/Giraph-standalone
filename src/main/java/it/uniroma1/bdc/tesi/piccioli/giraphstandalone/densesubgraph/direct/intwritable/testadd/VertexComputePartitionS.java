package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct.intwritable.testadd;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
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
 * Vertex in partition S Classe che viene eseguita anche in fase di init
 * (creazione incoming edge nei primi 2 supertep)
 */
public class VertexComputePartitionS extends BasicComputation<IntWritable, VertexValue, IntWritable, IntWritable> {

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
    public void compute(Vertex<IntWritable, VertexValue, IntWritable> vertex, Iterable<IntWritable> messages) throws IOException {
        Long superstep = this.getSuperstep();
        
        //check vertex status
        if (!vertex.getValue().getPartitionS().IsActive() && !vertex.getValue().getPartitionT().IsActive()) {
            vertex.voteToHalt();
        }

        if (superstep > 2) {

            //Partition S
            if (this.isEven(superstep)) {
                //2, 4, 6 ..
                if (vertex.getValue().getPartitionS().IsActive()) {

                    //CALCOLO SOGLIA S
                    LongWritable removedVertexInS = this.getAggregatedValue(REMOVEDVERTICIESINS);//superstep precedente	
                    LongWritable removedEdges = this.getAggregatedValue(REMOVEDEDGES);//superstep precedente

                    Long verticesInS = this.getTotalNumVertices() - removedVertexInS.get();
                    //EpSpSPp --> |E(S, T )| = |E ∩ (S×T)|

                    Long totEdge = this.getTotalNumEdges() / 2;
                    Long EpSTp = totEdge - removedEdges.get();

                    // soglia = (1 + epsilon) * (|E(S, T)| / |S| )
                    Double soglia = (1 + epsilon) * ((EpSTp.doubleValue()) / verticesInS.doubleValue());

                    //grado uscente è calcolato come la il numero di archi associati  al nodo meno il grado entrante del nodo
//                    int edgeRemoved = vertex.getValue().getPartitionS().getEdgeRemoved(); //vertici uscenti dal nodo eliminati in superstep precedenti
                    int outDegree = vertex.getValue().getOutDegree().get() - vertex.getValue().getPartitionS().getEdgeRemoved();
                    if (outDegree <= soglia) {
                        //elimino vertice dalla partizione S
                        vertex.getValue().getPartitionS().deactivate();
                        vertex.getValue().getPartitionS().setDeletedSuperstep(superstep);

                        this.aggregate(REMOVEDVERTICIESINS, new LongWritable(1));
                        this.aggregate(REMOVEDEDGES, new LongWritable(outDegree));

                        //invio messaggi a vertici in Partizione T che diminueranno il loro inDegree
                        for (Edge<IntWritable, IntWritable> edge : vertex.getEdges()) {
                            if (edge.getValue().get() == 0) {
                                this.sendMessage(edge.getTargetVertexId(), vertex.getId());
                            }
                        }
                    }
                }
            } else {
                //3,5,7 ..
                //vertici nella partizione T

                if (vertex.getValue().getPartitionT().IsActive()) {
                    //elimino da lista incoming edge
                    int edgeToRemove = Iterables.size(messages);

                    //aggiorno tot vertici rimossi da partizione
                    int totRemoved = vertex.getValue().getPartitionT().getEdgeRemoved() + edgeToRemove;
                    vertex.getValue().getPartitionT().setEdgeRemoved(totRemoved);

                    //Caso vertice rimane senza edge uscenti -> da eliminare
                    if (totRemoved == vertex.getValue().getInDegree().get()) {
                        vertex.getValue().getPartitionT().deactivate();
                        vertex.getValue().getPartitionT().setDeletedSuperstep(superstep - 1);
                        this.aggregate(REMOVEDVERTICIESINT, new LongWritable(1));
                    }
                }
            }
        } else {
            //INIT - Superstep 0 e 1 calcolo degree e "completo" grafo 
            if (superstep == 0) {
                vertex.getValue().setOutDegree(new IntWritable(vertex.getNumEdges()));
                this.sendMessageToAllEdges(vertex, vertex.getId());
            }
            if (superstep == 1) {

                for (IntWritable msg : messages) {
                    vertex.addEdge(EdgeFactory.create(msg, new IntWritable(1))); //TODO: è necessario costruire anche l'arco o è sufficiente il nodo entrane??

                }
                vertex.getValue().setInDegree(new IntWritable(Iterables.size(messages)));
            }
        }

    }

    private boolean isEven(long a) {
        return (a % 2 == 0);
    }
}
