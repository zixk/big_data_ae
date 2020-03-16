package mapreduce.wcv3;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {
	
	
	
	 
	
	 
	    public MyGroupComparator() {
	        super(MyKey.class, true);
	    }
	    
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable wc1, WritableComparable wc2) {
	        
	        MyKey key1 = (MyKey) wc1;
	        MyKey key2 = (MyKey) wc2;
	        return key1.term.compareTo(key2.term);
	    }
	}


