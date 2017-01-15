package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KDoubleArrayWritable implements WritableComparable<KDoubleArrayWritable>  {
    private String key;
    private int k;

    public KDoubleArrayWritable() {

    }

    public KDoubleArrayWritable(String keyArray, int k) {
        this.key = keyArray;
        this.k = k;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
    

    public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}

	@Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(key);
        out.writeInt(k);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key = in.readUTF();
        k = in.readInt();
    }

	@Override
	public int compareTo(KDoubleArrayWritable other) {
		return (toString().compareTo(other.toString()));
	}
	
	@Override
	public String toString(){
		return (key.length() > 0) ? key+","+k : String.valueOf(k);
	}
}