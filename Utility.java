package edu.buffalo.cse562;

public class Utility {
	public static MyTuple stringToTuple(String tupleString, MyTable t)
	{
		String[] values;
		MyTuple newTuple = null;
		if(tupleString != null){
			values = tupleString.split("\\|");
			newTuple = new MyTuple(t);
			int j = 0;
			for(String name: t.columnList){
				boolean isDouble = false;
				boolean isInteger = false;
				String nextType = t.columnTypes.get(name);
				if(nextType.equalsIgnoreCase("float")||nextType.equalsIgnoreCase("DECIMAL")){
					isDouble = true;
				}else if(nextType.equalsIgnoreCase("int")){
					isInteger = true;
				}

				MyColumn col = new MyColumn(name.toUpperCase(), values[j].trim(), isDouble, isInteger, t.tableName);
				newTuple.AddColumn(col);
				j++;
			}
		}
		return newTuple;

	}

}
