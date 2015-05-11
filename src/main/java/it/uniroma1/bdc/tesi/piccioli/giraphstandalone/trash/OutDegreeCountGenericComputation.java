package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trash;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class OutDegreeCountGenericComputation extends BasicComputation<WritableComparable, Writable, NullWritable, NullWritable> {

    @Override
    public void compute(
            Vertex<WritableComparable, Writable, NullWritable> vertex,
            Iterable<NullWritable> messages) throws IOException {
	
        vertex.setValue(new Text(Integer.toString(vertex.getNumEdges())));
        vertex.voteToHalt();
    }
}
