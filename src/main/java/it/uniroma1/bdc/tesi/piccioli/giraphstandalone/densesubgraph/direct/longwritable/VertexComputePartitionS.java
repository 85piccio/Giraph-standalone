package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct.longwritable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 *
 * @author piccio
 *
 * Vertex in partition S Classe che viene eseguita anche in fase di init (creazione incoming edge nei primi 2 supertep)
 */
public class VertexComputePartitionS extends BasicComputation<LongWritable, VertexValue, NullWritable, LongWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(VertexComputePartitionS.class);

    /**
     * Somma aggregator name
     */
    private static final String REMOVEDVERTICIESINS = "removedVerticiesFromS";
    private static final String REMOVEDEDGES = "removedEdges";

    private static final String SOGLIA = "soglia";

    @Override
    public void compute(Vertex<LongWritable, VertexValue, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {
	Long superstep = this.getSuperstep();

	if (superstep > 1) {

	    //Partition S
	    if (this.isEven(superstep)) {
		//2, 4, 6 ..
		if (vertex.getValue().getPartitionS().IsActive()) {

		    int outDegree = vertex.getNumEdges() - vertex.getValue().getPartitionS().getEdgeRemoved();
		    Double soglia = this.getContext().getConfiguration().getDouble(SOGLIA, Double.NEGATIVE_INFINITY);

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
		    for (LongWritable msg : messages) {
			vertex.getValue().getIncomingEdge().remove(msg.get());
		    }
		}
	    }

	} else {
	    //INIT - Superstep 0 e 1 creano lista incoming edge 
	    if (superstep == 0) {
		this.sendMessageToAllEdges(vertex, vertex.getId());
	    }
	    if (superstep == 1) {
		for (LongWritable msg : messages) {
		    vertex.getValue().getIncomingEdge().add(msg.get());
		}
	    }
	}

    }

    private boolean isEven(long a) {
	return (a % 2 == 0);
    }
}
