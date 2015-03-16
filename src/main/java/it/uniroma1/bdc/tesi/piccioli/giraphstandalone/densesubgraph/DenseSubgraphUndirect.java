/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph;

import java.io.IOException;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
    private static final String VERTECIES = "vertecies";
    private static final String EDGES = "edges";

    private static final String REMOVEDVERTICIES = "removedVerticies";
    private static final String SOGLIA = "soglia";

    @Override
    public void compute(Vertex<LongWritable, DenseSubgraphVertexValue, NullWritable> vertex, Iterable<LongWritable> itrbl) throws IOException {
        long superstep = getSuperstep();

        Double soglia = this.getContext().getConfiguration().getDouble(SOGLIA, Double.NaN);
        //degree del nodo
        Integer degree = vertex.getNumEdges();

//        //A(S) ← {i ∈ S | deg S (i) ≤ 2(1 + )ρ(S)}
//        if(degree <= soglia){
//            //elimino edge nodo
//            for(Edge<LongWritable,NullWritable> edge : vertex.getEdges()){
//                this.removeEdgesRequest(vertex.getId(), edge.getTargetVertexId());
//            }
//            //Elimino vertice
//            this.removeVertexRequest(vertex.getId());
//        }
        if (degree < soglia ) {
            vertex.getValue().setIsActive(Boolean.FALSE);
            vertex.getValue().setDeletedSuperstep(superstep);
            
            this.aggregate(REMOVEDVERTICIES, new IntWritable(1));

            vertex.voteToHalt();
        } else {
            this.aggregate(VERTECIES, new DoubleWritable(1));
            this.aggregate(EDGES, new DoubleWritable(degree));
        }
        
    }

}
