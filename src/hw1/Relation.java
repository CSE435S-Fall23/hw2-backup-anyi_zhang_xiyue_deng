package hw1;

import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	//table每一行的信息
	private TupleDesc td;
	//rename td tupledesc 是表头
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		this.tuples = l;
		this.td = td;
		//your code here
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 *              the num of colum to be selected
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		ArrayList<Tuple> selectedTuples = new ArrayList<>();

		for (Tuple tuple: tuples) {
			System.out.println(tuple.toString());
			Field tupleField = tuple.getField(field);
			if (tupleField.compare(op, operand)) {
				// Create a new Tuple with the selected field
				selectedTuples.add(tuple);
			}
		}
		// Create and return a new Relation with the selected tuples and the new TupleDesc
		System.out.println(selectedTuples.size());
		return new Relation(selectedTuples, this.td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		// Create a copy of the existing TupleDesc
		int length = this.td.numFields();
		Type[] newTypes = new Type[length];
		String[] newFields = new String[length];
		for(int i=0;i<this.td.numFields();i++){
			newTypes[i] = this.td.getType(i);
			newFields[i]= this.td.getFieldName(i);    ///array 从0开始算位数的。。。。是bua。。。。。。emmm
		}
		// Rename the specified fields
		for (int i = 0; i < fields.size(); i++) {
			int fieldIndex = fields.get(i);
			if (fieldIndex >= 0 && fieldIndex < newFields.length) {
				newFields[fieldIndex] = names.get(i);
			}
		}
		// Create a new TupleDesc with the updated field names
		TupleDesc newTd = new TupleDesc(newTypes, newFields);

		// Return a new Relation with the same tuples and the updated TupleDesc
		return new Relation(this.tuples, newTd);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		// Handle the case where no fields are selected (return an empty relation or a special value)
		if (fields == null || fields.isEmpty()) {
			throw new NoSuchElementException("nononononono");
		}
		// Create a new TupleDesc for the resulting relation
		Type[] selectedTypes = new Type[fields.size()];
		String[] selectedNames = new String[fields.size()];

		for (int i = 0; i < fields.size(); i++) {
			int fieldIdx = fields.get(i);
			if (fieldIdx >= 0 && fieldIdx < td.numFields()) {
				selectedTypes[i] = td.getType(fieldIdx);
				selectedNames[i] = td.getFieldName(fieldIdx);
			}
		}

		TupleDesc newTd = new TupleDesc(selectedTypes, selectedNames);

		// Create a list for the new tuples
		ArrayList<Tuple> newTuples = new ArrayList<>();

		for (Tuple tuple : tuples) {
			// Create a new tuple for the resulting relation
			Tuple newTuple = new Tuple(newTd);

			// Set the fields for the new tuple based on the selected fields
			for (int i = 0; i < fields.size(); i++) {
				int fieldIdx = fields.get(i);
				if (fieldIdx >= 0 && fieldIdx < td.numFields()) {
					newTuple.setField(i, tuple.getField(fieldIdx));
				}
			}

			newTuples.add(newTuple);
		}

		return new Relation(newTuples, newTd);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition 即this.column
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition 即other.column
	 * @return
	 */
	//only inner join  include left join or not??????
	//when this.column = other.column
	public Relation join(Relation other, int field1, int field2) {
		//>的问题,field1，2输错问题和用不了
		if (field1 < 0 || field1 > this.td.numFields() || field2 < 0 || field2 > other.getDesc().numFields()) {
			throw new NoSuchElementException("no join!");
		}
		//创造新的合并的行
		int numFields = this.td.numFields() + other.getDesc().numFields();
		Type[] newTypes = new Type[numFields];
		String[] newNames = new String[numFields];
		//分别将td和other的type和name塞入
		for (int i = 0; i < td.numFields(); i++) {
			newTypes[i] = td.getType(i);
			newNames[i] = td.getFieldName(i);
		}
		for (int i = 0; i < other.getDesc().numFields(); i++) {
			newTypes[td.numFields() + i] = other.getDesc().getType(i);
			newNames[td.numFields() + i] = other.getDesc().getFieldName(i);
		}
		//创建新的td包含this和other的
		TupleDesc newTd = new TupleDesc(newTypes, newNames);
		// 新列表
		ArrayList<Tuple> newTuples = new ArrayList<>();
		for (Tuple tuple1 : tuples) {
			for (Tuple tuple2 : other.getTuples()) {
				// 检查两个元组是否满足联接条件
				if (tuple1.getField(field1).equals(tuple2.getField(field2))) {
					// 创建一个新元组，包含两个关系的所有字段
					Tuple newTuple = new Tuple(newTd);

					// 将第一个tuple复制到新元组,也是先加td后加other
					for (int i = 0; i < td.numFields(); i++) {
						newTuple.setField(i, tuple1.getField(i));
					}
					// 将第二个tuple复制到新元组
					for (int i = 0; i < other.getDesc().numFields(); i++) {
						newTuple.setField(td.numFields() + i, tuple2.getField(i));
					}
					newTuples.add(newTuple);
				}
			}
		}

		//your code here
        return new Relation(newTuples, newTd);
    }
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		Aggregator newagg = new Aggregator(op,groupBy,td);
		for(int i=0;i<tuples.size();i++){
			newagg.merge(tuples.get(i));
		}
		return new Relation(newagg.getResults(),td);
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("TupleDesc: ");
		sb.append(td.toString());
		sb.append("\n");

		for (Tuple tuple : tuples) {
			sb.append(tuple.toString());
			sb.append("\n");
		}
		//your code here

		return sb.toString();
	}
}
