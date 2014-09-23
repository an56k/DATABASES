package edu.buffalo.cse562;

import java.io.IOException;

import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class MyTupleSerializer implements Serializer<MyTuple>{

	int size;
	MyTable table;
	public MyTupleSerializer(int size, MyTable tbl)
	{
		this.size = size;
		table = tbl;
	}
	@Override
	public MyTuple deserialize(SerializerInput in) throws IOException,
			ClassNotFoundException {
		String stringTuple = "";
       //System.out.println("I am here");
		for(int i=0; i<size-1; i++)
		{
			stringTuple += in.readUTF()+"|";
		}
		stringTuple += in.readUTF();

		return Utility.stringToTuple(stringTuple, table);
	}

	@Override
	public void serialize(SerializerOutput out, MyTuple tuple)
			throws IOException {
		
		for(int i=0; i<size; i++)
		{
			out.writeUTF(tuple.getColumn(i).getValue());
		}
	}

}
