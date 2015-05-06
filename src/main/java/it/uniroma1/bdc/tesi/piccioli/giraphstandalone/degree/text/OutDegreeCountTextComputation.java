package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.degree.text;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class OutDegreeCountTextComputation extends BasicComputation<Text, Text, NullWritable, NullWritable> {

    @Override
    public void compute(
            Vertex<Text, Text, NullWritable> vertex,
            Iterable<NullWritable> messages) throws IOException {
	
        vertex.setValue(new Text(Integer.toString(vertex.getNumEdges()))); 
        vertex.voteToHalt();
    }
}
