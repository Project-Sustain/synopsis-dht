package sustain.synopsis.ingestion.client.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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



}
