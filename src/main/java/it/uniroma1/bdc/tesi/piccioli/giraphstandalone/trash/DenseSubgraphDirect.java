package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trash;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct.longwritable.VertexValue;
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
public class DenseSubgraphDirect extends BasicComputation<LongWritable, VertexValue, NullWritable, LongWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(DenseSubgraphDirect.class);

    /**
     * Somma aggregator name
     */
    private static final String REMOVEDVERTICIESINS = "removedVerticiesFromS";
    private static final String REMOVEDEDGES = "removedEdges";
    private static final String REMOVEDVERTICIESINT = "removedVerticiesFromT";
//    private static final String REMOVEDEDGESINT = "removedEdgesFromT";

    private static final String SOGLIA = "soglia";
    private static final String PARTITIONTOPROCESS = "partitionToProcess";

    @Override
    public void compute(Vertex<LongWritable, VertexValue, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {
	Long superstep = this.getSuperstep();
	if (superstep > 1) {
	    String partition = this.getContext().getConfiguration().getStrings(PARTITIONTOPROCESS)[0];

	    Double soglia = this.getContext().getConfiguration().getDouble(SOGLIA, Double.NEGATIVE_INFINITY);
	    if (partition.compareTo("S") == 0) {
		//Partition S
		if (this.isEven(superstep)) {
		    //2, 4, 6 ..
		    if (vertex.getValue().getPartitionS().IsActive()) {

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
			for (LongWritable msg : messages) {
			    vertex.getValue().getIncomingEdge().remove(msg.get());
			}
		    }
		}

	    } else if (partition.compareTo("T") == 0) {//Partition T
		if (this.isEven(superstep)) {
		    //2, 4, 6 ..
//
		    if (vertex.getValue().getPartitionT().IsActive()) {
			int inDegree = vertex.getValue().getIncomingEdge().size();

			if (inDegree <= soglia) {
			    //rimuovo vertice da partizione T
			    vertex.getValue().getPartitionT().deactivate();
			    vertex.getValue().getPartitionT().setDeletedSuperstep(superstep);
			    this.aggregate(REMOVEDVERTICIESINT, new LongWritable(1));
			}

			for (Long inEdge : vertex.getValue().getIncomingEdge()) {
			    this.sendMessage(new LongWritable(inEdge), vertex.getId());
			}
			vertex.getValue().getIncomingEdge().clear();
		    }
		} else {
		    //3,5,7 ..

		    int edgeToRemove = 0;
		    for (LongWritable msg : messages) {
			edgeToRemove++;
		    }
		    this.aggregate(REMOVEDEDGES, new LongWritable(edgeToRemove));

		    //aggiorno outDegree S-->T
		    int edgeRemoved = vertex.getValue().getPartitionS().getEdgeRemoved();
		    vertex.getValue().getPartitionS().setEdgeRemoved(edgeRemoved + edgeToRemove);
		}
	    } else {
		//Throw an error
	    }

	} else {
	    //Superstep 0 e 1 creano lista incoming edge
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
