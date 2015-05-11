///*
// * To change this license header, choose License Headers in Project Properties.
// * To change this template file, choose Tools | Templates
// * and open the template in the editor.
// */
//package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trash;
//
//import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.DenseSubgraphUndirect;
//import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.DenseSubgraphUndirectVertexValue;
//import java.io.IOException;
//import org.apache.giraph.edge.Edge;
//import org.apache.giraph.graph.BasicComputation;
//import org.apache.giraph.graph.Vertex;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.log4j.Logger;
//
///**
// *
// * @author piccio
// */
//public class DenseSubgraphUndirect_old extends BasicComputation<LongWritable, DenseSubgraphUndirectVertexValue, NullWritable, LongWritable> {
//
//    /**
//     * Class logger
//     */
//    private static final Logger LOG = Logger.getLogger(DenseSubgraphUndirect.class);
//    /**
//     * Somma aggregator name
//     */
////    private static final String VERTECIES = "vertecies";
////    private static final String EDGES = "edges";
//
//    private static final String REMOVEDVERTICIES = "removedVerticies";
//    private static final String REMOVEDEDGES = "removedEdges";
//    private static final String SOGLIA = "soglia";
//
//    @Override
//    public void compute(Vertex<LongWritable, DenseSubgraphUndirectVertexValue, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {
//        long superstep = this.getSuperstep();
////        System.out.println("Vertice "+vertex.getId()+" si trova superstep " + superstep);
//
//        int edgeToRemove = 0;
//
//        if (vertex.getValue().IsActive()) {
//
//            Double soglia = this.getContext().getConfiguration().getDouble(SOGLIA, Double.NaN);
//            //degree del nodo
//            Integer degree = vertex.getNumEdges();
//
//            //Aggiorno struttura info "vertici ancora attivi"
//            for (LongWritable msg : messages) {
//                if (!vertex.getValue().getEdgeRemoved().contains(msg.get())) {
//                    vertex.getValue().getEdgeRemoved().add(msg.get());
//                    edgeToRemove++;
//                }
//            }
//
////        //A(S) ← {i ∈ S | deg S (i) ≤ 2(1 + )ρ(S)}
//            if (degree <= soglia) {
//                //rimozione logica dei vertici
//                vertex.getValue().deactive();
//                vertex.getValue().setDeletedSuperstep(superstep);
//
//                //rimozione logica del vertice
//                this.aggregate(REMOVEDVERTICIES, new LongWritable(1));
//                //rimozione logica dei Edge (solo quelli verso vertici ancora attivi, non eliminati in superstep precedenti)
//                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
//                    if (!vertex.getValue().getEdgeRemoved().contains(edge.getTargetVertexId().get())) {
//                        //mando messaggio a nodi vicini di considerare l'edge rimosso
//                        this.sendMessageToAllEdges(vertex, vertex.getId());
//                        vertex.getValue().getEdgeRemoved().add(edge.getTargetVertexId().get());
//                        //Rimuovo 
//                        edgeToRemove++;
//                    }
//                }
//                vertex.voteToHalt();
//            }
//
//            if (edgeToRemove != 0) {
//                this.aggregate(REMOVEDEDGES, new LongWritable(edgeToRemove));
//            }
////        if (degree <= soglia) {
////            //elimino edge nodo
////            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
////                this.removeEdgesRequest(vertex.getId(), edge.getTargetVertexId());
////            }
////            //Elimino vertice
////            this.removeVertexRequest(vertex.getId());
////            this.aggregate(REMOVEDVERTICIES, new IntWritable(1));
////            vertex.voteToHalt();
////        }
//
//        }
//    }
//
//}
