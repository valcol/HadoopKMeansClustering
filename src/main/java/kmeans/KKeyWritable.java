package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KKeyWritable implements WritableComparable<KKeyWritable>  {
    private String clusterId;
    private int k;

    public KKeyWritable() {

    }

    public KKeyWritable(String clusterId, int k) {
        this.clusterId = clusterId;
        this.k = k;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
    
    public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}

	@Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(clusterId);
        out.writeInt(k);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clusterId = in.readUTF();
        k = in.readInt();
    }

	@Override
	public int compareTo(KKeyWritable other) {
		return (toString().compareTo(other.toString()));
	}
	
	@Override
	public String toString(){
		return (clusterId.length() > 0) ? clusterId+","+k : String.valueOf(k);
	}
}