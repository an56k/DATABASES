package edu.buffalo.cse562;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class KeyArraySerializer implements Serializer<ArrayList<String>> {

	@Override
	public ArrayList<String> deserialize(SerializerInput in)
			throws IOException, ClassNotFoundException {
		String[] spl = in.readUTF().split(";");
		ArrayList<String> keys = new ArrayList<String>(spl.length);
		for (int i=0; i<spl.length; i++)
		{
			keys.add(spl[i]);
		}
		return keys;
	}

	@Override
	public void serialize(SerializerOutput out, ArrayList<String> arrStr)
			throws IOException {
		String str ="";
		int i=0;
		for(; i< arrStr.size()-1; i++){
           str += arrStr.get(i) + ";";
		}
		
		str += arrStr.get(i);
		out.writeUTF(str);
	}
	

}
