package synopsis2.client.geohash;

import java.util.concurrent.Semaphore;

/**
 * @author Thilina Buddhika
 */
public class Test {
    public static Semaphore sem = new Semaphore(1);

    public static void main(String[] args) {
        try {
            sem.acquire();
            sem.release();
            if(sem.availablePermits() == 0) {
                sem.release();
            }
            System.out.println(sem.tryAcquire());
            System.out.println(sem.tryAcquire());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
