package mapreduce.wcv3;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {
	
	
	

	/**
	 * Composite key formed by a Natural Key (state) and Secondary Keys (city, total).
	 * 
	 * @author Nicomak
	 *
	 */
	

		public String term;
		public String frequency;
		

		public MyKey() {
		}

		public MyKey(String term, String frequency) {
			super();
			this.set(term, frequency);
		}

		public void set(String term, String frequenc) {
			this.term = (term == null) ? "" : term;
			this.frequency = (frequency == null) ? "" : frequency;
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(term);
			out.writeUTF(frequency);
			
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			term = in.readUTF();
			frequency = in.readUTF();
			
		}

		@Override
		public int compareTo(MyKey o) {
			int termCmp = term.toLowerCase().compareTo(o.term.toLowerCase());// first, ordering by term 
			if (termCmp != 0) {
				return termCmp;
			} else {
				
				int splitIndex1 = frequency.toString().indexOf(":");
				Integer termFrequency1= Integer.parseInt(frequency.toString().substring(splitIndex1+1));// extract the number (12) from the value for example: docid:12  
				int splitIndex2 = o.frequency.toString().indexOf(":");
				Integer termFrequency2= Integer.parseInt(o.frequency.toString().substring(splitIndex1+1));
				
				int frequencyCmp = termFrequency1.compareTo(termFrequency1); // second ordering by value
				if (frequencyCmp != 0) {
					return (-1 * frequencyCmp); // descending order
				} 
			}
			
			return 0;
		}

	}


