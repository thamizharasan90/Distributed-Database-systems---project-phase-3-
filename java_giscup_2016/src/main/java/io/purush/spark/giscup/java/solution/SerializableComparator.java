package io.purush.spark.giscup.java.solution;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author Purush Swaminathan
 * @param <T>
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable {
    static<T> SerializableComparator<T> serialize(SerializableComparator<T> comparator){
        return comparator;
    }
}
