package hw1;

import java.util.*;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	private AggregateOperator operator;
	private boolean groupBy;
	private TupleDesc tupleDesc;
	private ArrayList<Tuple> result;

	//if not groupby, return one result
	private int count;
	private int sum;
	//if groupby initiate these two
	private HashMap<Field, Integer> sumList;
	private HashMap<Field, Integer> countList;

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		this.operator = o;
		this.groupBy = groupBy;
		this.tupleDesc = td;

		//initate final result
		this.result = new ArrayList<>();

		//initiate for storage
		this.sum = 0;
		this.count = 0;
		this.sumList = new HashMap<>(); //for different groups
		this.countList = new HashMap<>();
		//your code here
	}
	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		if(!groupBy) {
			//for both int and string
			if (operator == AggregateOperator.MAX) {
				if(result.size()!=0){
					if(result.get(0).getField(0).compare(RelationalOperator.LT,t.getField(0))){
						result.get(0).setField(0,t.getField(0));
					}
				}
				else{
					result.add(t);
				}
			}
			else if(operator == AggregateOperator.MIN){
				if(result.size()!=0){
					if(result.get(0).getField(0).compare(RelationalOperator.GT,t.getField(0))){
						result.get(0).setField(0,t.getField(0));
					}
				}
				else{
					result.add(t);
				}

			}
			else if(operator == AggregateOperator.SUM){
				if(t.getDesc().getType(0).equals(Type.STRING)){
					throw new NoSuchElementException("not valid for string");
				}
				else{
					if(t.getDesc().numFields() == 2){
						System.out.println("!Sum");
						System.out.println(this.sum);
						this.sum= this.sum +((IntField) t.getField(1)).getValue();
					}
					else{
						this.sum= this.sum +((IntField) t.getField(0)).getValue();
					}

				}
			}
			else if(operator == AggregateOperator.COUNT){
				this.count  += 1;

			}
			else if(operator == AggregateOperator.AVG){
				if(t.getDesc().getType(0).equals(Type.STRING)){
					throw new NoSuchElementException("not valid for string");
				}
				else{
					this.sum= this.sum +((IntField) t.getField(0)).getValue();
					this.count  += 1;
				}

			}

		}
		if(groupBy){  //input tuple t has two fields, first to group by , second to aggregate
			int num = -1;
			for(int i =0; i<result.size();i++){
				if(result.get(i).getField(0).equals(t.getField(0))){
					num = i; // find the index of the row that has the same first field
					break;
				}
			}
			//这个改了莫。。。。
			if (operator == AggregateOperator.MAX) {
				if(result.size()!=0){
					if(result.get(num).getField(1).compare(RelationalOperator.LT,t.getField(1))){
						result.get(num).setField(1,t.getField(1));
					}
				}
				else{
					result.add(t);//------------------------------也是add t？？？？？？？？？？
				}
			}
			else if(operator == AggregateOperator.MIN){
				if(result.size()!=0){
					if(result.get(num).getField(1).compare(RelationalOperator.GT,t.getField(1))){
						result.get(num).setField(1,t.getField(1));
					}
				}
				else{
					result.add(t);
				}

			}
			else if(operator == AggregateOperator.SUM){
				if(t.getDesc().getType(1).equals(Type.STRING)){
					throw new NoSuchElementException("not valid for string");
				}
				else{
					if(result.size()!=0){
						int sumtotal = ((IntField) result.get(num).getField(1)).getValue() +((IntField) t.getField(1)).getValue();
						sumList.put(t.getField(0),sumtotal);
					}
					else{
						int sumtotal2 =  ((IntField) t.getField(1)).getValue();
						sumList.put(t.getField(0),sumtotal2);
					}
				}
			}
			else if(operator == AggregateOperator.COUNT){
				if(result.size()!=0){
					int addCount = ((IntField) result.get(num).getField(1)).getValue() +1;
					countList.put(t.getField(0),addCount);
				}
				else{
					int count2 =  1;
					sumList.put(t.getField(0),count2);
				}

			}
			else if(operator == AggregateOperator.AVG){
				if(t.getDesc().getType(1).equals(Type.STRING)){
					throw new NoSuchElementException("not valid for string");
				}
				else{
					if(result.size()!=0) {
						int sumtotal = ((IntField) result.get(num).getField(1)).getValue() + ((IntField) t.getField(1)).getValue();
						sumList.put(t.getField(0), sumtotal);
						int addCount = ((IntField) result.get(num).getField(1)).getValue() + 1;
						countList.put(t.getField(0), addCount);
					}
					else{
						int sumtotal3 =  ((IntField) t.getField(1)).getValue();
						sumList.put(t.getField(0),sumtotal3);
						int count3 =  1;
						sumList.put(t.getField(0),count3);
					}
				}

			}

		}

			//your code here

	}
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//for GT, LT, already updated result,
		// for SUM, Count, Average, need convert sum/sumlist, cout/counlist into tuple/tuplelist
		if(!groupBy){
			if(operator == AggregateOperator.SUM){
				Tuple t = new Tuple(tupleDesc);
				if(t.getDesc().numFields() == 2){
					t.setField(1,new IntField(this.sum));
					result.add(t);
				}
				else{
					t.setField(0,new IntField(this.sum));
					result.add(t);
				}

			}
			else if(operator == AggregateOperator.COUNT){
				Tuple t = new Tuple(tupleDesc);
				t.setField(0,new IntField(this.count));
				result.add(t);
			}
			else if(operator == AggregateOperator.AVG){
				int average = this.sum/this.count;  ///.............................这么算ok？？
				Tuple t = new Tuple(tupleDesc);
				t.setField(0,new IntField(average));
				result.add(t);
			}
		}
		if(groupBy){
			if(operator == AggregateOperator.SUM){
				for (Field key : sumList.keySet()) {
					Tuple t = new Tuple(tupleDesc);
					t.setField(0,key);
					t.setField(1,new IntField(sumList.get(key)));
					result.add(t);
				}
			}
			else if(operator == AggregateOperator.COUNT){
				for (Field key : countList.keySet()) {
					Tuple t = new Tuple(tupleDesc);
					t.setField(0,key);
					t.setField(1,new IntField(countList.get(key)));
					result.add(t);
				}
			}
			else if(operator == AggregateOperator.AVG){
				for (Field key : sumList.keySet()) {
					Tuple t = new Tuple(tupleDesc);
					t.setField(0,key);
					t.setField(1,new IntField(sumList.get(key)/countList.get(key)));
					result.add(t);
				}
			}

		}

		//your code here
		return result;
	}

}
