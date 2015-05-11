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

    private static final String SOGLIA = "soglia";

    @Override
    public void compute(Vertex<IntWritable, VertexValue, NullWritable> vertex, Iterable<IntWritable> messages) throws IOException {
	Long superstep = this.getSuperstep();
//	System.out.println("T");

	if (superstep > 1) {

	    if (this.isEven(superstep)) {
		//2, 4, 6 ..
//
		if (vertex.getValue().getPartitionT().IsActive()) {
		    int inDegree = vertex.getValue().getIncomingEdge().size();
		    Double soglia = this.getContext().getConfiguration().getDouble(SOGLIA, Double.NEGATIVE_INFINITY);

		    if (inDegree <= soglia) {
			//rimuovo vertice da partizione T
			vertex.getValue().getPartitionT().deactivate();
			vertex.getValue().getPartitionT().setDeletedSuperstep(superstep);
			this.aggregate(REMOVEDVERTICIESINT, new LongWritable(1));
		    }

		    for (Integer inEdge : vertex.getValue().getIncomingEdge()) {
			this.sendMessage(new IntWritable(inEdge), vertex.getId());
		    }
		    vertex.getValue().getIncomingEdge().clear();
		}
	    } else {
		//3,5,7 ..
		if (vertex.getValue().getPartitionS().IsActive()) {

		    int edgeToRemove = Iterables.size(messages);
		    this.aggregate(REMOVEDEDGES, new LongWritable(edgeToRemove));

		    //aggiorno outDegree S-->T
		    int edgeRemoved = vertex.getValue().getPartitionS().getEdgeRemoved();
		    vertex.getValue().getPartitionS().setEdgeRemoved(edgeRemoved + edgeToRemove);
		}
	    }

	}

    }

    private boolean isEven(long a) {
	return (a % 2 == 0);
    }
}
