package sustain.synopsis.ingestion.client.core;

import junit.framework.Assert;
import org.junit.jupiter.api.Test;

public class LRUCacheTest {

    private final String s1 = "s1";
    private final String s2 = "s2";
    private final String s3 = "s3";
    private final String s4 = "s4";

    @Test
    public void testSimple() {
        LRUCache<String> lruCache = new LRUCache<>();
        Assert.assertEquals(0,lruCache.size());

        lruCache.use(s1);
        Assert.assertEquals(1,lruCache.size());

        Assert.assertEquals(s1, lruCache.evictLRU());
        Assert.assertEquals(0, lruCache.size());
    }

    @Test
    public void test2() {
        LRUCache<String> lruCache = new LRUCache<>();

        lruCache.use(s1);
        lruCache.use(s2);

        Assert.assertEquals(s1, lruCache.evictLRU());
    }

    @Test
    public void test3() {
        LRUCache<String> lruCache = new LRUCache<>();

        lruCache.use(s1);
        lruCache.use(s2);
        lruCache.use(s1);

        Assert.assertEquals(s2, lruCache.evictLRU());
    }

    @Test
    public void test4() {
        LRUCache<String> lruCache = new LRUCache<>();

        lruCache.use(s1);
        lruCache.use(s2);
        lruCache.use(s3);
        lruCache.use(s4);
        lruCache.use(s1);

        Assert.assertEquals(s2, lruCache.evictLRU());
    }



}
