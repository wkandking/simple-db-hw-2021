package simpledb.optimizer;

import simpledb.execution.Predicate;

import javax.print.DocFlavor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int width; // 记录等宽直方图的宽度

    private int ntups;

    private Map<Integer, List<Integer>> histogram; // 将直方图保存为map


    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = Math.min(buckets, max - min + 1); // 处理桶大于区间的一种方法
        this.min = min;
        this.max = max;
        this.width = (max - min + 1) / this.buckets;
        this.ntups = 0;
        this.histogram = new HashMap<>(buckets);
        for(int i = 0; i < buckets; i++){
            histogram.put(i, new ArrayList<>());
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v < this.min || v > this.max) throw new IllegalArgumentException("v 小于 min 或 大于 max ***** from IntHistogram.es");
        int index = (v - min) / width;
        if(index == buckets){
            index = buckets - 1;
        }
        List<Integer> list = histogram.get(index);
        list.add(v);
        histogram.put(index, list);
        ntups ++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int index = (v - min) / width;
        double result = 0.00;
        List<Integer> list = null;
        switch (op){
            case EQUALS:
                result = estimateEqualSelectivity(index);
                break;
            case GREATER_THAN:
                int right = index == buckets - 1 ? max : min + (index + 1) * width - 1;
                list = histogram.getOrDefault(index, new ArrayList<>());
                int height = list.size();
                result = height / (ntups + 0.00) * ((right - v) / (width + 0.00));
                for(int i = index + 1; i < buckets; i++){
                    result += estimateEqualSelectivity(i);
                }
                break;
            case LESS_THAN:
                int left = min + index * width;
                list = histogram.getOrDefault(index, new ArrayList<>());
                result = list.size() / (ntups + 0.00) * ((v - left) / (width + 0.00));
                for(int i = 0; i < index; i++){
                    result += estimateEqualSelectivity(i);
                }
                break;
            case LESS_THAN_OR_EQ:
                for(int i = 0; i <= index; i++){
                    result += estimateEqualSelectivity(i);
                }
                break;
            case GREATER_THAN_OR_EQ:
                for(int i = index; i < buckets; i++){
                    result += estimateEqualSelectivity(i);
                }
                break;
            case NOT_EQUALS:
                for(int i = 0; i < buckets; i++){
                    if(i == index) continue;
                    result += estimateEqualSelectivity(i);
                }
                break;
            default:
                throw new IllegalArgumentException("op 值有问题 ***** from IntHistogram.es");
        }
    	// some code goes here
        return result;
    }
    public double estimateEqualSelectivity(int index){
        double result = 0.00;
        List<Integer> list = list = histogram.getOrDefault(index, new ArrayList<>());
        int height = list.size();
        return (height / (width + 0.00)) / (ntups + 0.00);
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "buckets = " + this.buckets + "min = " + this.min + "max = " + this.max;
    }
}
