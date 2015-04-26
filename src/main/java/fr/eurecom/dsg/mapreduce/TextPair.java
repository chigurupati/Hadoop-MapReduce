package fr.eurecom.dsg.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


/**
 * TextPair is a Pair of Text that is Writable (Hadoop serialization API)
 * and Comparable to itself.
 *
 */
public class TextPair implements WritableComparable<TextPair> {

// TODO: add the pair objects as TextPair fields
    private Text first = new Text();
    private Text second = new Text();
    private String delim = ",";

    public void set(Text first, Text second) {
        // TODO: implement the set method that changes the Pair content
        this.first = first;
        this.second =  second;
    }

    public Text getFirst() {
        // TODO: implement the first getter
        return this.first;
    }

    public Text getSecond() {
        // TODO: implement the second getter
        return this.second;
    }

    public TextPair() {
        // TODO: implement the constructor, empty constructor MUST be implemented
        //       for deserialization
        first = new Text("");
        second = new Text("");
    }

    public TextPair(String first, String second) {
        this.set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        this.set(first, second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO: write to out the serialized version of this such that
        //       can be deserializated in future. This will be use to write to HDFS
        //out.writeBytes(first.toString());
        //out.writeBytes(second.toString());

        this.first.write(out);
        this.second.write(out);
        System.out.println(first.toString()+":::"+second.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO: read from in the serialized version of a Pair and deserialize it

        if(this.first != null && this.second != null){
            this.first.readFields(in);
            this.second.readFields(in);
        }
        System.out.println(first+":::"+second);
        System.out.println(in.toString());

    }

    @Override
    public int hashCode() {
        // TODO: implement hash
        return first.hashCode() * 200 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        // TODO: implement equals

        boolean isEqual = false;
        if (obj instanceof TextPair) {
            TextPair iPair = (TextPair)obj;
            if(first != null && second != null){
                isEqual = first.equals(iPair.first) && second.equals(iPair.second);
            }
        }
        return isEqual;
    }

    /*@Override
    public int compareTo(TextPair tp) {
        // TODO: implement the comparison between this and tp
        int cmp = first.compareTo(tp.getFirst());
        if (0 == cmp) {
            cmp = second.compareTo(tp.getSecond());
        }
        return cmp;
    }*/
    
    @Override
    public int compareTo(TextPair tp) {
       /* if (first.toString().equals(tp.first.toString())) {
            return second.toString().compareTo(tp.second.toString());
        } else {
            return first.toString().compareTo(tp.first.toString());
        }*/
        
        int returnVal = this.first.compareTo(tp.getFirst());
	    if(returnVal != 0){
	        return returnVal;
	    }
	    
	    if(this.second.toString().equals("*")){
	        return -1;
	    }else if(tp.getSecond().toString().equals("*")){
	        return 1;
	    }
	    
	    return this.second.compareTo(tp.getSecond());
    }

    @Override
    public String toString() {
        // TODO: implement toString for text output format
        return "" + first + delim + second;
    }





// DO NOT TOUCH THE CODE BELOW

    /** Compare two pairs based on their values */
    public static class Comparator extends WritableComparator {

        /** Reference to standard Hadoop Text comparator */
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {
        WritableComparator.define(TextPair.class, new Comparator());
    }

    /** Compare just the first element of the Pair */
    public static class FirstComparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair) a).getFirst().compareTo(((TextPair) b).getFirst());
            }
            return super.compare(a, b);
        }

    }
}