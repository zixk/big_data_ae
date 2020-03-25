package mapreduce.wcv3;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComperator extends WritableComparator {
	/**
	 * This class handles the secondary sort of terms and their frequencies
	 * 
	 */
	protected CompositeKeyComperator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text ip1 = (Text) w1;
		Text ip2 = (Text) w2;
		int cmp = ip1.toString().compareTo(ip2.toString());
		return cmp;
	}
}
