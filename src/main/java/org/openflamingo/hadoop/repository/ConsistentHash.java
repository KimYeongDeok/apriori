package org.openflamingo.hadoop.repository;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ConsistentHash<T> {
    private final HashFunction hashFunction;
    private final int numberOfReplicas;
    private final SortedMap<Integer, T> circle = new TreeMap<Integer, T>();

    public ConsistentHash(HashFunction hashFunction,
                          int numberOfReplicas, Collection<T> nodes) {
        this.hashFunction = hashFunction;
        this.numberOfReplicas = numberOfReplicas;

        for (T node : nodes) {
            add(node);
        }
    }

    public void add(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            HashCode hashCode = hashFunction.hashString(node.toString()+i);
            circle.put(hashCode.hashCode(), node);
        }
    }

    public void remove(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            HashCode hashCode = hashFunction.hashString(node.toString()+i);
            circle.remove(hashCode.hashCode());
        }
    }

    public T get(Object key) {
        if (circle.isEmpty()) {
            return null;
        }
        HashCode hashCode = hashFunction.hashString(key.toString());
        Integer hash = hashCode.hashCode();
        System.out.println(circle.get(hash));
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, T> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ?
                           circle.firstKey() :  tailMap.firstKey();
        }
        return circle.get(hash);
    }
}
