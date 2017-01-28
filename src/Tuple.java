import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Tuple implements WritableComparable<Tuple> {
	Text userA;
	Text userB;

	public Tuple() {
		this.userA = new Text();
		this.userB = new Text();
	}
	public Tuple(String userA, String userB) {
		int val = userA.compareTo(userB);
		if(val < 0) {
			this.userA = new Text(userA);
			this.userB = new Text(userB);
		} else {
			this.userA = new Text(userB);
			this.userB = new Text(userA);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		userA.readFields(in);
		userB.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		userA.write(out);
		userB.write(out);
	}

	@Override
	public int compareTo(Tuple o) {
		return (this.userA.toString()+this.userB.toString()).compareTo(o.userA.toString()+o.userB.toString());
	}
}