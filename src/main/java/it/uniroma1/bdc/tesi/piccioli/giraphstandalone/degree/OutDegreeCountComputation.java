package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.degree;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class OutDegreeCountComputation extends BasicComputation<Text, LongWritable, NullWritable, Text> {

    @Override
    public void compute(
            Vertex<Text, LongWritable, NullWritable> vertex,
            Iterable<Text> messages) throws IOException {

        vertex.setValue(new LongWritable(vertex.getNumEdges()));
        vertex.voteToHalt();
    }
}
