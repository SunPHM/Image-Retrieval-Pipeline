package ir.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TextVectorWritable  implements Writable {
	Text filepath = new Text();
	VectorWritable vw = new VectorWritable();

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		filepath.readFields(in);
		vw.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		filepath.write(out);
		vw.write(out);
		
	}
	public void set(String file, Vector vw){
		this.filepath.set(file);
		this.vw.set(vw);
	}
	public String toString(){
		return filepath.toString() + "\t" + vw.toString();
	}
	public Text getText(){return filepath;}
	public VectorWritable getVectorWritable(){return vw;}

}
