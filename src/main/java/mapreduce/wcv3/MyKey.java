package mapreduce.wcv3;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {

	/**
	 * Creates composite key formed by a primary key(term) and a secondary key(docid, frequency)
	 *
	 */

	public String tag;
	public String term;
	public String docid;
	public String frequency;

	public MyKey() {
	}

	public MyKey(String tag, String term, String docid, String frequency) {
		super();
		this.set(tag, term, docid, frequency);
	}

	public void set(String tag, String term, String docid, String frequency) {
		this.tag = (tag == null) ? "" : tag;
		this.term = (term == null) ? "" : term;
		this.docid = (docid == null) ? "" : docid;
		this.frequency = (frequency == null) ? "" : frequency;

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(tag);
		out.writeUTF(term);
		out.writeUTF(docid);
		out.writeUTF(frequency);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tag = in.readUTF();
		term = in.readUTF();
		docid = in.readUTF();
		frequency = in.readUTF();

	}

	@Override
	public int compareTo(MyKey o) {
		int termCmp = term.toLowerCase().compareTo(o.term.toLowerCase());

		if (termCmp != 0) {
			return termCmp;

		}

		return 0;// this.docid.compareTo(o.docid);
	}
}
