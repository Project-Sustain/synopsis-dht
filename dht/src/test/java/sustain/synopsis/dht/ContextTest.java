package sustain.synopsis.dht;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ContextTest {
    @TempDir
    Path tempDir;

    @Test
    @Order(1)
    void testSingleton(){
        Context ctx = Context.getInstance();
        // we do not override the equal(). Therefore, this should do an object comparison
        Assertions.assertEquals(ctx, Context.getInstance());
    }

    @Test
    @Order(0) // we need to make sure this test is run first before the ctx is initialized by other test cases
    void testThrowErrorWhenAccessedUninitializedContext(){
        Context context = Context.getInstance();
        // accessing an uninitialized context
        Assertions.assertThrows(RuntimeException.class, context::getNodeConfig);
    }

    @Test
    @Order(2)
    void testInitializeWithFilePath() throws IOException {
        File configFile = tempDir.resolve("nodeconfig.yaml").toFile();
        NodeConfigurationTest.serializeNodeConfig(configFile);

        NodeConfiguration expectedConfig = NodeConfiguration.fromYamlFile(configFile.getAbsolutePath());
        // initialize the context with a file pointer
        Context context = Context.getInstance();
        context.initialize(configFile.getAbsolutePath());
        Assertions.assertEquals(expectedConfig, context.getNodeConfig());
    }

    @Test
    @Order(3)
    void testInitializeWithNodeConfigObject() throws IOException {
        File configFile = tempDir.resolve("nodeconfig.yaml").toFile();
        NodeConfigurationTest.serializeNodeConfig(configFile);

        NodeConfiguration expectedConfig = NodeConfiguration.fromYamlFile(configFile.getAbsolutePath());
        Context ctx = Context.getInstance();
        ctx.initialize(expectedConfig);

        Assertions.assertEquals(expectedConfig, ctx.getNodeConfig());
    }

    @Test
    @Order(4)
    void testProperties(){
        Context ctx = Context.getInstance();
        Assertions.assertNull(ctx.getProperty("test_prop_key"));
        ctx.setProperty("test_prop_key", "test_prop_val");
        Assertions.assertEquals("test_prop_val", ctx.getProperty("test_prop_key"));
    }

    @Test
    @Order(5)
    void testRingAccess(){
        Context ctx = Context.getInstance();
        Assertions.assertNull(ctx.getRing());
        Ring ring = new Ring();
        ctx.setRing(ring);
        Assertions.assertEquals(ring, ctx.getRing());
    }
}
