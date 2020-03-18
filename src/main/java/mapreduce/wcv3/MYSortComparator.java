package mapreduce.wcv3;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MYSortComparator extends WritableComparator{
	
	 
	    public MYSortComparator() {
	        super(MyKey.class, true);
	    }
	    
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable wc1, WritableComparable wc2) {
	        
	    	MyKey key1 = (MyKey) wc1;
	    	MyKey key2 = (MyKey) wc2;
	        
	    	int termCmp = key1.term.toLowerCase().compareTo(key2.term.toLowerCase());// first, ordering by term 
			if (termCmp != 0) {
				return termCmp;
			} else {
				
				//int splitIndex1 = frequency.toString().indexOf(":");
				//Integer termFrequency1= Integer.parseInt(frequency.toString().substring(splitIndex1+1));// extract the number (12) from the value for example: docid:12  
				//int splitIndex2 = o.frequency.toString().indexOf(":");
				//Integer termFrequency2= Integer.parseInt(o.frequency.toString().substring(splitIndex1+1));
				
				int frequencyCmp = key1.frequency.compareTo(key2.frequency); // second ordering by value
				if (frequencyCmp != 0) {
					return (-1 * frequencyCmp); // descending order
				} 
			}
			
			return 0;
	    }
	}


