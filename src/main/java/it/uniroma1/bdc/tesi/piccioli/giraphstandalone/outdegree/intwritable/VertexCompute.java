package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.outdegree.intwritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/*
 *  INPUT FORMAT - IntIntNullNullInputFormat.java
 *  OUTPUT FORMAT - VertexWithIntValueNullEdgeTextOutputFormat.java
 */
public class VertexCompute extends BasicComputation<IntWritable, IntWritable, NullWritable, NullWritable> {

    @Override
    public void compute(
            Vertex<IntWritable, IntWritable, NullWritable> vertex,
            Iterable<NullWritable> messages) throws IOException {

        vertex.setValue(new IntWritable(vertex.getNumEdges()));
        vertex.voteToHalt();
    }
}
