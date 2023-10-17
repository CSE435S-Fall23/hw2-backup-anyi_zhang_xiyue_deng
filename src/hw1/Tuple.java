package hw1;

import java.sql.Types;


/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	
	
	private TupleDesc td;
	private Field[] fields;
	private int id;
	private int pid;

	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		this.fields = new Field[t.numFields()];
		this.td = t;
		//your code here
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		
		//your code here
		return pid;
	}

	public void setPid(int pid) {
		 this.pid=pid;
		//your code here
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return id;
	}

	public void setId(int id) {
		this.id = id;
		//your code here
	}
	
	public void setDesc(TupleDesc td) {
		this.td=td;
		//your code here;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		if(i<fields.length) {
			this.fields[i]=v;
		}
		//your code here
	}
	
	public Field getField(int i) {
		//your code here
		return fields[i];
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		String present = "";
		for(int i =0;i<fields.length;i++) {
			String str = fields[i].toString();
			present+=str+"\t";
		}
		return present;
	}
}
	