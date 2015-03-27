package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct;

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
 */
public class DenseSubgraphDirect extends BasicComputation<LongWritable, DenseSubgraphDirectVertexValue, NullWritable, LongWritable> {

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
    public void compute(Vertex<LongWritable, DenseSubgraphDirectVertexValue, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {
	Long superstep = this.getSuperstep();
	if (superstep > 1) {
	    String partition = this.getContext().getConfiguration().getStrings(PARTITIONTOPROCESS)[0];

	    //dbug
	    System.out.println("case partition\t" + partition);

	    Double soglia = this.getContext().getConfiguration().getDouble(SOGLIA, 0.0);
	    if (partition.compareTo("S") == 0) {
		//Partition S
		if (this.isEven(superstep)) {
		    //2, 4, 6 ..
		    if (vertex.getValue().getPartitionS().IsActive()) {

			int outDegree = vertex.getNumEdges() - vertex.getValue().getPartitionS().getEdgeRemoved();

			if (outDegree <= soglia) {
			    //elimino vertice dalla partizione S
			    vertex.getValue().getPartitionS().deactive();
			    vertex.getValue().getPartitionS().setDeletedSuperstep(superstep);

			    this.aggregate(REMOVEDVERTICIESINS, new LongWritable(1));

			    //invio messaggi a vertici in Partizione T che diminueranno il loro inDegree
			    this.sendMessageToAllEdges(vertex, vertex.getId());
			    this.aggregate(REMOVEDEDGES, new LongWritable(outDegree));
			}
		    }

//		    //Calcolo degree
//		    //degree = n edge che hanno come destinazione un vertice della partizione T
//		    int vertexOutDegreeT = vertex.getNumEdges() - vertex.getValue().getPartitionT().getEdgeRemoved();
//		    int vertexOutDegreeS = vertex.getNumEdges() - vertex.getValue().getPartitionS().getEdgeRemoved();
//		    if (vertexOutDegreeT <= soglia) {
//			vertex.getValue().getPartitionS().deactive();
//			//"eliminato" dalla partizione S durante superstep
//			vertex.getValue().getPartitionS().setDeletedSuperstep(superstep);
//			this.aggregate(REMOVEDVERTICIESINS, new LongWritable(1));
//			//Segnalo ad incoming edge che nodo è stato eliminato dalla partizione S
//			for (Long inEdge : vertex.getValue().getIncomingEdge()) {
//			    this.sendMessage(new LongWritable(inEdge), vertex.getId());
//			}
//
//			//sengalo con id negativo per distinguerli da msg precedenti 
//			this.sendMessageToAllEdges(vertex, new LongWritable(-vertex.getId().get()));
//
//			this.aggregate(REMOVEDEDGESINS, new LongWritable(vertexOutDegreeS));
//		    }
		} else {
		    //3,5,7 ..
		    //vertici nella partizione T

		    //elimino da lista incoming edge
//		    Set tmp = vertex.getValue().getIncomingEdge();
		    if (vertex.getValue().getPartitionT().IsActive()) {
			for (LongWritable msg : messages) {
			    vertex.getValue().getIncomingEdge().remove(msg.get());
			}
		    }

//		    int outEdgeToRemove = 0;
//		    int inEdgeToRemove = 0;
//		    //messaggi da dest outcoming edge che sono stati eliminati durante supertesp precedete
//		    for (LongWritable msg : messages) {
//			if (msg.get() > 0) {
//			    outEdgeToRemove++;
//			} else {
//			    inEdgeToRemove++;
//			}
//
//		    }
//		    vertex.getValue().getPartitionS().setEdgeRemoved(vertex.getValue().getPartitionS().getEdgeRemoved() - outEdgeToRemove);
//		    vertex.getValue().getPartitionT().setEdgeRemoved(vertex.getValue().getPartitionT().getEdgeRemoved() - inEdgeToRemove);
//		    this.aggregate(REMOVEDEDGESINS, new LongWritable(outEdgeToRemove));
		}

	    } else if (partition.compareTo("T") == 0) {//Partition T
		if (this.isEven(superstep)) {
		    //2, 4, 6 ..
//
		    if (vertex.getValue().getPartitionT().IsActive()) {
			int inDegree = vertex.getValue().getIncomingEdge().size();

			if (inDegree <= soglia) {
			    //rimuovo vertice da partizione T
			    vertex.getValue().getPartitionT().deactive();
			    vertex.getValue().getPartitionT().setDeletedSuperstep(superstep);
			    this.aggregate(REMOVEDVERTICIESINT, new LongWritable(1));
			}

			for (Long inEdge : vertex.getValue().getIncomingEdge()) {
			    this.sendMessage(new LongWritable(inEdge), vertex.getId());
			}
			 vertex.getValue().getIncomingEdge().clear();
		    }
//		    //Calcolo degree
//		    //vertexInDegreeS _ n incoming edge con vertice sorgente in S
//		    int vertexInDegreeS = vertex.getValue().getIncomingEdge().size() - vertex.getValue().getPartitionS().getEdgeRemoved();
//		    int vertexOutDegreeT = vertex.getNumEdges() - vertex.getValue().getPartitionT().getEdgeRemoved();
//
//		    if (vertexInDegreeS <= soglia) {
//			vertex.getValue().getPartitionT().deactive();
//			vertex.getValue().getPartitionT().setDeletedSuperstep(superstep);
//			this.aggregate(REMOVEDVERTICIESINT, new LongWritable(1));
//
//			for (Long inEdge : vertex.getValue().getIncomingEdge()) {
//			    this.sendMessage(new LongWritable(inEdge), vertex.getId());
//			}
//			this.aggregate(REMOVEDEDGESINT, new LongWritable(vertexOutDegreeT));
//		    }
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

		    //		    int edgeToRemove = 0;
		    //		    for (LongWritable msg : messages) {
		    //			edgeToRemove++;
		    //		    }
		    //		    vertex.getValue().getPartitionT().setEdgeRemoved(vertex.getValue().getPartitionT().getEdgeRemoved() - edgeToRemove);
		    //		    this.aggregate(REMOVEDEDGESINT, new LongWritable(edgeToRemove));
		    {

		    }
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
