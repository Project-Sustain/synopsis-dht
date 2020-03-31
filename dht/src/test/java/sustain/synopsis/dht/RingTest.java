package sustain.synopsis.dht;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.dispersion.RingIdMapper;
import sustain.synopsis.dht.zk.MembershipTracker;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RingTest {
    @Mock
    MembershipTracker membershipTrackerMock;

    @Test
    void testRingEntityCompare() {
        Ring.Entity e1 = new Ring.Entity(BigInteger.valueOf(1000), "localhost:8080", 0);
        Ring.Entity e2 = new Ring.Entity(BigInteger.valueOf(1000), "localhost:8080", 0);
        Ring.Entity e3 = new Ring.Entity(BigInteger.valueOf(1100), "localhost:8080", 0);
        Ring.Entity e4 = new Ring.Entity(BigInteger.valueOf(900), "localhost:8080", 0);

        Assertions.assertEquals(0, e1.compareTo(e2));
        Assertions.assertTrue(e1.compareTo(e3) < 0);
        Assertions.assertTrue(e1.compareTo(e4) > 0);
    }

    @Test
    void testRingLookup() {
        MockitoAnnotations.initMocks(this);
        // simple key to id conversion scheme for unit testing.
        // All keys are string representations of integers which are parsed into BigInteger instances
        RingIdMapper simpleDispersionScheme = key -> BigInteger.valueOf(Long.parseLong(key) % 1000);
        Ring ring = new Ring(simpleDispersionScheme, membershipTrackerMock);
        List<Ring.Entity> entities = new ArrayList<>();
        entities.add(new Ring.Entity(simpleDispersionScheme.getIdentifier("200"), "localhost:200", 0));
        entities.add(new Ring.Entity(simpleDispersionScheme.getIdentifier("300"), "localhost:300", 0));
        entities.add(new Ring.Entity(simpleDispersionScheme.getIdentifier("600"), "localhost:600", 0));
        entities.add(new Ring.Entity(simpleDispersionScheme.getIdentifier("900"), "localhost:900", 0));
        entities.add(new Ring.Entity(simpleDispersionScheme.getIdentifier("1100"), "localhost:1100", 0));
        ring.updateRing(entities);

        Assertions.assertEquals(5, ring.getSize());
        Assertions.assertEquals(ring.lookup("0"), "localhost:1100");
        Assertions.assertEquals(ring.lookup("100"), "localhost:1100");
        Assertions.assertEquals(ring.lookup("150"), "localhost:200");
        Assertions.assertEquals(ring.lookup("700"), "localhost:900");
        Assertions.assertEquals(ring.lookup("950"), "localhost:1100");

        // update the ring
        List<Ring.Entity> update = new ArrayList<>();
        update.add(new Ring.Entity(simpleDispersionScheme.getIdentifier("1000"), "localhost:1000", 0));
        ring.updateRing(update);

        Assertions.assertEquals(6, ring.getSize());
        Assertions.assertEquals(ring.lookup("0"), "localhost:1000");
        Assertions.assertEquals(ring.lookup("1"), "localhost:1100");
        Assertions.assertEquals(ring.lookup("950"), "localhost:1000");
    }

    @Test
    void testNodeStringToEntity() {
        MockitoAnnotations.initMocks(this);
        // using a simple id mapper for comprehensibility
        RingIdMapper simpleDispersionScheme = key -> BigInteger.valueOf(Long.parseLong(key.split(":")[1]) % 1000);
        Ring ring = new Ring(simpleDispersionScheme, membershipTrackerMock);
        Ring.Entity entity = ring.convertToEntity("localhost:900:0");
        Assertions.assertEquals(BigInteger.valueOf(900L), entity.getId());
        Assertions.assertEquals("localhost:900", entity.getAddr());
        Assertions.assertEquals(0, entity.getVirtualId());
    }

    @Test
    void testRingAutoUpdate() throws InterruptedException {
        MockitoAnnotations.initMocks(this);
        RingIdMapper simpleDispersionScheme = key -> BigInteger.valueOf(Long.parseLong(key.split(":")[1]) % 1000);
        Ring ring = new Ring(simpleDispersionScheme, membershipTrackerMock);
        Mockito.doNothing().when(membershipTrackerMock).subscribe(ring);
        Mockito.doNothing().when(membershipTrackerMock).getAvailableWorkers();
        Thread ringUpdater = new Thread(ring);
        ringUpdater.start();

        // List of updates
        List<String> initialNodeList = new ArrayList<>();
        initialNodeList.add("localhost:200:0");
        initialNodeList.add("localhost:300:0");
        initialNodeList.add("localhost:600:0");
        initialNodeList.add("localhost:900:0");
        initialNodeList.add("localhost:1100:0");
        ring.handleMembershipChange(initialNodeList);

        // ring updates are asynchronous
        while (ring.getSize() == 0) {
            Thread.sleep(1000);
        }

        // keys are appended and prepended with ':' to get the simple RingIdMapper implementation to work
        Assertions.assertEquals(5, ring.getSize());
        Assertions.assertEquals(ring.lookup(":0:"), "localhost:1100");
        Assertions.assertEquals(ring.lookup(":100:"), "localhost:1100");
        Assertions.assertEquals(ring.lookup(":150:"), "localhost:200");
        Assertions.assertEquals(ring.lookup(":700:"), "localhost:900");
        Assertions.assertEquals(ring.lookup(":950:"), "localhost:1100");

        // update the ring again
        ring.handleMembershipChange(Collections.singletonList("localhost:1000:0"));
        while (ring.getSize() == 5) {
            Thread.sleep(1000);
        }
        // see if the update is reflected
        Assertions.assertEquals(6, ring.getSize());
        Assertions.assertEquals(ring.lookup(":0:"), "localhost:1000");

        ringUpdater.interrupt();
        while (ringUpdater.isAlive()) {
            Thread.sleep(1000);
        }
    }
}
