package go.driss.pn.utils;

import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;

public class LRUCache<K, V> {
    private static final float hashTableLoadFactor = 0.75f;
    private LinkedHashMap<K, V> map;
    private int cacheSize;

    public LRUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        int hashTableCapacity = (int) Math
                .ceil(cacheSize / hashTableLoadFactor) + 1;
        map = new LinkedHashMap<K, V>(hashTableCapacity, hashTableLoadFactor,
                true) {
            private static final long serialVersionUID = 1;

            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > LRUCache.this.cacheSize;
            }
        };
    }

    public synchronized V get(K key) {
        return map.get(key);
    }

    public synchronized void put(K key, V value) {
        map.put(key, value);
    }

    public synchronized void clear() {
        map.clear();
    }

    public synchronized int usedEntries() {
        return map.size();
    }

    public synchronized Collection<Map.Entry<K, V>> getAll() {
        return new ArrayList<Map.Entry<K, V>>(map.entrySet());
    }

    public static void main(String[] args) {
        LRUCache<String, String> c = new LRUCache<String, String>(2);
        c.put("1", "one"); // 1
        c.put("2", "two"); // 2 1
        c.put("3", "three"); // 3 2 1
        c.put("4", "four"); // 4 3 2
        System.out.println(c.getAll());
    }
}
