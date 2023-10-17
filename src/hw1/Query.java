package hw1;

import java.util.*;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();

		Catalog c = Database.getCatalog();
		//From
		FromItem fromItem = sb.getFromItem();
		String tableName1 = fromItem.toString();
		int tid1 = c.getTableId(tableName1);
		HeapFile hf1 =  c.getDbFile(tid1);
		//System.out.println("***");
		//System.out.println(tableName1);
		//System.out.println(hf1.getAllTuples().toString());
		//System.out.println("+++");
		//System.out.println(hf1.getAllTuples().size());
		Relation rl1 = new Relation(hf1.getAllTuples(),c.getTupleDesc(tid1));

		//Join
		List<Join> joins = sb.getJoins();
		if(joins!=null) {
			//Relation rljoin = new Relation(hf1.getAllTuples(),c.getTupleDesc(tid1));
			for (Join join : joins) {
				// Access join details such as join type, right table, and join condition
				//System.out.println("join**");
				String rightTable = join.getRightItem().toString();
				//System.out.println(rightTable);
				Expression onExp = join.getOnExpression();
				EqualsTo equalsTo = (EqualsTo) onExp;
				Column col1 = (Column) equalsTo.getLeftExpression();
				ColumnVisitor cv1 = new ColumnVisitor();
				String name1 = col1.getColumnName();
				//System.out.println(name1);
				Column col2 = (Column) equalsTo.getRightExpression();
				String name2 = col2.getColumnName();
				//System.out.println("---------");
				//System.out.println(onExp.toString());
				int rightTid = c.getTableId(rightTable);
				HeapFile hfright = c.getDbFile(rightTid);
				//System.out.println(hfright.getAllTuples().toString());
				Relation rlright = new Relation(hfright.getAllTuples(), c.getTupleDesc(rightTid));
				//List<Column> columnList = join.getUsingColumns();
				//System.out.println(join.getUsingColumns());
				//String name1 = columnList.get(0).getColumnName();
				//String name2 = columnList.get(1).getColumnName();
				int field1 = hf1.getTupleDesc().nameToId(name1);
				int field2 = hfright.getTupleDesc().nameToId(name2);
				rl1 = rl1.join(rlright, field1, field2);
			}
		}
		//System.out.println(rl1.getTuples().size());
		//System.out.println(rl1.getTuples().toString());
		//Where
		Expression where = sb.getWhere();
		if(where!=null) {
			WhereExpressionVisitor wv = new WhereExpressionVisitor();
			where.accept(wv);
			int whereleft = hf1.getTupleDesc().nameToId(wv.getLeft());
			rl1 = rl1.select(whereleft, wv.getOp(), wv.getRight());
		}
		// Aggregate
		List<SelectItem> selectItems = sb.getSelectItems();
		if(selectItems.get(0).toString() != "*"){
			ArrayList<Integer> columnList = new ArrayList<>();
			ArrayList<Integer> renameList = new ArrayList<>();
			ArrayList<String> newnameList = new ArrayList<>();
			//Relation rlAggr = rlwhere;
			for (SelectItem selectItem : selectItems) {
				ColumnVisitor cv = new ColumnVisitor();
				selectItem.accept(cv);
				String colname = cv.getColumn();
				//System.out.println(colname);
				int colnum = rl1.getDesc().nameToId(colname);
				//int colnum = hf1.getTupleDesc().nameToId(colname);
				columnList.add(colnum);
				if(cv.isAggregate()){
					List<Expression> groupbyExpressions  = sb.getGroupByColumnReferences();
					if(groupbyExpressions!=null){
						//String groupbycol = groupbyExpressions.get(0).toString();
						rl1 = rl1.aggregate(cv.getOp(),true);
					}
					else{
						System.out.println("????");
						System.out.println(rl1.getTuples().toString());
						rl1 = rl1.aggregate(cv.getOp(),false);
						System.out.println(cv.getOp().toString());
						System.out.println("======");
						//System.out.println(rl1.getTuples().toString());
					}
				}
				SelectExpressionItem selectEx = (SelectExpressionItem) selectItem;
				if(selectEx.getAlias()!=null){
					String newName = selectEx.getAlias().getName();
					renameList.add(colnum);
					newnameList.add(newName);
				}

			}
			System.out.println(rl1.getTuples().size());

			//Select
			rl1 = rl1.project(columnList);
			System.out.println(rl1.getTuples().size());

			//As
			rl1 = rl1.rename(renameList,newnameList);
		}



		return rl1;
		
	}
}
