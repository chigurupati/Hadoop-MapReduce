package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;
import org.apache.hadoop.io.Writable;
/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntMapWritable implements Writable {

    // TODO: add an internal field that is the real associative array
    public final Map<String, Integer> counts;
    
    public StringToIntMapWritable() {
        this(new HashMap<String, Integer>());
    }
    public StringToIntMapWritable(Map<String, Integer> counts) {
        this.counts = counts;
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        // TODO: implement deserialization

        // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
        // StringToIntMapWritable when reading new records. Remember to initialize your variables
        // inside this function, in order to get rid of old data.
        counts.clear();
        String inLine = in.readLine();
        if (inLine != null) {
            StringTokenizer tokenizer = new StringTokenizer(inLine, " ");
            while (tokenizer.hasMoreElements()) {
                String occurence = tokenizer.nextToken();
                String occurenceWord = occurence.substring(0, occurence.lastIndexOf("-"));
                int occurenceCount = Integer.parseInt(occurence.substring(occurence.lastIndexOf("-") + 1));
                counts.put(occurenceWord, occurenceCount);
            }
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {

        // TODO: implement serialization
         for(String s: counts.keySet()){
            out.write((s + "-" + counts.get(s) + " ").getBytes());
         }
    }
    
    @Override
    public String toString() {
        StringBuffer s = new StringBuffer();
        for(String key: new TreeSet<String>(counts.keySet())){
            s.append((key + "-" + counts.get(key) + " "));
        }
        return s.toString();
    }
    
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((counts == null) ? 0 : counts.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StringToIntMapWritable other = (StringToIntMapWritable) obj;
        if (counts == null) {
            if (other.counts != null)
                return false;
        } else if (!counts.equals(other.counts))
            return false;
        return true;
    }
    
}