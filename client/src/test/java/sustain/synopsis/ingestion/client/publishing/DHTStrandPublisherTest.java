package sustain.synopsis.ingestion.client.publishing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DHTStrandPublisherTest {

    static TestIngestService serviceA;
    static TestIngestService serviceB;
    static Thread t1;
    static Thread t2;

//    @BeforeAll
//    static void setup() throws InterruptedException {
//        Map<String, String> hostMapping = new HashMap<>();
//        hostMapping.put("b", "localhost:44002");
//
//        serviceA = new TestIngestService(44001, hostMapping);
//        serviceB = new TestIngestService(44002);
//
//        t1 = new Thread(serviceA);
//        t2 = new Thread(serviceB);
//
//        t1.start();
//        t2.start();
//
//        serviceA.latch.await();
//        serviceB.latch.await();
//    }
//
//    @Test
//    void test() throws InterruptedException {
//        DHTStrandPublisher dhtStrandPublisher = new DHTStrandPublisher("localhost:44001", "test-dataset", 1);
//        List<Strand> send = new ArrayList<>();
//
//        Path path = new Path(new Feature("asdf", 1.0f));
//        path.get(path.size() - 1).setData(new DataContainer(new RunningStatisticsND(new double[]{1})));
//
//        Strand a = new Strand("a", 0, 100, path);
//        Strand b = new Strand("b", 0, 100, path);
//        send.add(a);
//        send.add(b);
//
//        dhtStrandPublisher.publish(send);
//
//        dhtStrandPublisher.defaultMyChannel.limiter.acquire();
//
//        Assertions.assertEquals(1, dhtStrandPublisher.myChannelMap.size());
//    }

}
