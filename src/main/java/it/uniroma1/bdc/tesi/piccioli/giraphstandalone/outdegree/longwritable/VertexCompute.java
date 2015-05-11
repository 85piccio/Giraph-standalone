package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.outdegree.longwritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/*
 *  INPUT FORMAT - LongWritableLongWritableNullTextInputFormat.java
 *  OUTPUT FORMAT - VertexWithLongWritableValueAndKeyNullEdgeTextOutputFormat.java
 */
public class VertexCompute extends BasicComputation<LongWritable, LongWritable, NullWritable, NullWritable> {

    @Override
    public void compute(
            Vertex<LongWritable, LongWritable, NullWritable> vertex,
            Iterable<NullWritable> messages) throws IOException {

        vertex.setValue(new LongWritable(vertex.getNumEdges()));
        vertex.voteToHalt();
    }
}
