package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

	private int aggType;
	
  public IntAggregator(int what) {
    // some code goes here
  	if (aggType < Aggregator.MIN || aggType > Aggregator.COUNT)
  		throw new RuntimeException("Unrecognized Aggregator Type");
  	aggType = what;
  }

  /**
   * Takes a list of IntFields and returns an aggregate.  The specific
   * aggregate we're computing depends on the first parameter to our
   * Constructor.
   */
  public Field execute(List list) {
  	if (list.size() == 0) {
  		return IntField.createIntField(0);
  	}
  	
    // some code goes here
  	switch(aggType) {
  	case Aggregator.MIN:
  		return computeMin(list);
  	case Aggregator.MAX:
  		return computeMax(list);
  	case Aggregator.COUNT:
  		return IntField.createIntField(list.size());
  	case Aggregator.SUM:
  		return computeSum(list);
  	case Aggregator.AVG:
  		return computeAvg(list);
  		default:
  			throw new RuntimeException("shouldn't be here");
  	}
  }
  
  private IntField computeMin(List list) {
  	int min = Integer.MAX_VALUE;
  	
  	List<IntField> intList = (List<IntField>)list;
  	for (IntField f : intList) {
  		if (f.val() < min) {
  			min = f.val();
  		}
  	}
  	return IntField.createIntField(min);
  }
  
  private IntField computeMax(List list) {
  	int max = Integer.MIN_VALUE;
  	
  	List<IntField> intList = (List<IntField>)list;
  	for (IntField f : intList) {
  		if (f.myVal > max) {
  			max = f.myVal;
  		}
  	}
  	return IntField.createIntField(max);
  }
  
  private IntField computeSum(List list) {
  	int sum = 0;
  	
  	List<IntField> intList = (List<IntField>)list;
  	for (IntField f : intList) {
  		sum += f.myVal;
  	}
  	return IntField.createIntField(sum);
  }
  
  private IntField computeAvg(List list) {
  	int sum = computeSum(list).val();
  	int avg = sum / list.size();
  	
  	return IntField.createIntField(avg);
  }
}
