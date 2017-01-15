package kmeans;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class KValueWritable implements Writable {
	
	private String content;
	private String label;
	private double mesure;
	private ArrayList<Double> coordinates;
	
	public KValueWritable() {
		this.content = "";
		this.coordinates = new ArrayList<Double>();
		this.label = "";
		this.mesure = Double.MAX_VALUE;
	}
	
	public KValueWritable(String content, String label, double mesure, ArrayList<Double> coordinates) {
		super();
		this.content = content;
		this.label = label;
		this.mesure = mesure;
		this.coordinates = coordinates;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}


	public String getLabel() {
		return label;
	}


	public void setLabel(String label) {
		this.label = label;
	}


	public double getMesure() {
		return mesure;
	}


	public void setMesure(double mesure) {
		this.mesure = mesure;
	}


	public ArrayList<Double> getCoordinates() {
		return coordinates;
	}


	public void setCoordinates(ArrayList<Double> coordinates) {
		this.coordinates = coordinates;
	}


	public void readFields(DataInput in) throws IOException {
		
		int length = in.readInt();

        Double[] data = new Double[length];

        for(int i = 0; i < length; i++) {
            data[i] = in.readDouble();
        }
        
        coordinates =new ArrayList<Double>(Arrays.asList(data));
        
		content=in.readUTF();
		
		label=in.readUTF();
		
		mesure=in.readDouble();
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
	        
		out.writeUTF(content);
		
		out.writeUTF(label);
		
		out.writeDouble(mesure);
	}


}