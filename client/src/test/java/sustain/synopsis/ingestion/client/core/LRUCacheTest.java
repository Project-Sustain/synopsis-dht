package sustain.synopsis.ingestion.client.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class LRUCacheTest {

    private final String s1 = "s1";
    private final String s2 = "s2";
    private final String s3 = "s3";
    private final String s4 = "s4";

    @Test
    public void testSimple() {
        LRUCache<String> lruCache = new LRUCache<>();
        Assertions.assertEquals(0,lruCache.size());

        lruCache.use(s1);
        Assertions.assertEquals(1,lruCache.size());

        Assertions.assertEquals(s1, lruCache.evictLRU());
        Assertions.assertEquals(0, lruCache.size());
    }

    @Test
    public void test2() {
        LRUCache<String> lruCache = new LRUCache<>();

        lruCache.use(s1);
        lruCache.use(s2);

        Assertions.assertEquals(s1, lruCache.evictLRU());
    }

    @Test
    public void test3() {
        LRUCache<String> lruCache = new LRUCache<>();

        lruCache.use(s1);
        lruCache.use(s2);
        lruCache.use(s1);

        Assertions.assertEquals(s2, lruCache.evictLRU());
    }

    @Test
    public void test4() {
        LRUCache<String> lruCache = new LRUCache<>();

        lruCache.use(s1);
        lruCache.use(s2);
        lruCache.use(s3);
        lruCache.use(s4);
        lruCache.use(s1);

        Assertions.assertEquals(s2, lruCache.evictLRU());
    }

    @Test
    public void testEvictMultiple() {
        LRUCache<String> lruCache = new LRUCache<>();

        lruCache.use(s1);
        lruCache.use(s2);
        lruCache.use(s3);

        // this is one ugly line
        List<String> strings = lruCache.evictLRU(2);

        Assertions.assertEquals(strings.get(0), s1);
        Assertions.assertEquals(strings.get(1), s2);
    }



}
