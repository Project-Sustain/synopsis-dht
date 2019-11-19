package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SSTableTest {

    @Test
    void getUnitCountTest(){
        SSTable ssTable= new SSTable();
        Assertions.assertEquals(1, ssTable.getUnitCount(0, 1000));
        Assertions.assertEquals(1, ssTable.getUnitCount(100, 1000));
        Assertions.assertEquals(2, ssTable.getUnitCount(2000, 1000));
        Assertions.assertEquals(3, ssTable.getUnitCount(2100, 1000));
    }

    @Test
    void getEntriesPerBlockTest(){
        SSTable ssTable = new SSTable();
        Assertions.assertEquals(50, ssTable.entryCountForCurrentBlock(125, 50, 0));
        Assertions.assertEquals(50, ssTable.entryCountForCurrentBlock(125, 50, 1));
        Assertions.assertEquals(25, ssTable.entryCountForCurrentBlock(125, 50, 2));
        Assertions.assertEquals(0, ssTable.entryCountForCurrentBlock(0, 50, 0));
        Assertions.assertEquals(25, ssTable.entryCountForCurrentBlock(25, 50, 0));
    }
}
