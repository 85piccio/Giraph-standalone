package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.degree;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class OutDegreeCountComputation extends BasicComputation<Text, Text, NullWritable, Text>  {

    @Override
    public void compute(
            Vertex<Text, Text, NullWritable> vertex,
            Iterable<Text> messages) throws IOException {
        if (getSuperstep() == 0) {
            Iterable<Edge<Text, NullWritable>> edges = vertex.getEdges();
            for (Edge<Text, NullWritable> edge : edges) {
                sendMessage(edge.getTargetVertexId(), new Text("0"));
            }
        } else {
            Long sum = 0L;
            for (Text message : messages) {
                sum = sum + 1;
            }
            
            vertex.setValue(new Text(sum.toString()));
            vertex.voteToHalt();
        }
    }
}
