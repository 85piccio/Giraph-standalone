package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trianglecount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextAndDegree implements WritableComparable {

    Text id;
    IntWritable degree;

    public TextAndDegree() {
    }

    public TextAndDegree(Text id, IntWritable degree) {
        this.id = id;
        this.degree = degree;
    }

    public Text getId() {
        return id;
    }

    public void setId(Text id) {
        this.id = id;
    }

    public IntWritable getDegree() {
        return degree;
    }

    public void setDegree(IntWritable degree) {
        this.degree = degree;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        degree.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        degree.readFields(in);
    }

    @Override
    public int compareTo(Object o) {
        TextAndDegree ob = (TextAndDegree) o;
        return degree.compareTo(ob.getDegree());
    }
}
