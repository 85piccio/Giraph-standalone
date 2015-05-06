package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.degree.intwritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class OutDegreeCountComputation extends BasicComputation<IntWritable, IntWritable, NullWritable, NullWritable> {

    @Override
    public void compute(
            Vertex<IntWritable, IntWritable, NullWritable> vertex,
            Iterable<NullWritable> messages) throws IOException {

        vertex.setValue(new IntWritable(vertex.getNumEdges()));
        vertex.voteToHalt();
    }
}
