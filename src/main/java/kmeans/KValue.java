package kmeans;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class KValue implements Writable {
	
	private String content;
	private ArrayList<Double> coordinates;
	
	public KValue() {
		this.content = "";
		this.coordinates = new ArrayList<Double>();
	}
	 
	public KValue(String content, ArrayList<Double> coordinates) {
		this.content = content;
		this.coordinates = coordinates;
	}

	public String getContent() {
		return content;
	}

	public ArrayList<Double> getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(ArrayList<Double> coordinates) {
		this.coordinates = coordinates;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public void readFields(DataInput in) throws IOException {
		
		int length = in.readInt();

        Double[] data = new Double[length];

        for(int i = 0; i < length; i++) {
            data[i] = in.readDouble();
        }
        
        coordinates =new ArrayList<Double>(Arrays.asList(data));
        
		content=in.readLine();
	}

	public void write(DataOutput out) throws IOException {
		
	    int length = 0;
	    Double[] data = new Double[coordinates.size()];
	    data = coordinates.toArray(data);
	    
        if(data != null) {
            length = data.length;
        }

        out.writeInt(length);

        for(int i = 0; i < length; i++) {
            out.writeDouble(data[i]);
        }
	        
		out.writeBytes(content);
	}


}