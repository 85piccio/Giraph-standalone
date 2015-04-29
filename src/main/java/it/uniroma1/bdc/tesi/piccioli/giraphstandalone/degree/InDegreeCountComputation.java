package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.degree;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;

import java.io.IOException;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class InDegreeCountComputation extends BasicComputation<Text, Text, NullWritable, ByteWritable> {

    @Override
    public void compute(
            Vertex<Text, Text, NullWritable> vertex,
            Iterable<ByteWritable> messages) throws IOException {
        if (getSuperstep() == 0) {
            this.sendMessageToAllEdges(vertex, new ByteWritable());
        } else {
            Long sum = 0L;
            for (ByteWritable message : messages) {
                sum = sum + 1;
            }

            vertex.setValue(new Text(sum.toString()));
            vertex.voteToHalt();
        }
    }
}
