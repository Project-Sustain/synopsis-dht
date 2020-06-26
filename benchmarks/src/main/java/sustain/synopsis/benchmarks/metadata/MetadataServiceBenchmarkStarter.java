package sustain.synopsis.benchmarks.metadata;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import sustain.synopsis.metadata.MetadataService;
import sustain.synopsis.metadata.MetadataServiceRequestProcessor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MetadataServiceBenchmarkStarter {

    static void initLogFile(String logFilePath) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(logFilePath)))) {
            for (int datasetId = 0; datasetId < 100; datasetId++) {
                for (int sessionId = 0; sessionId < 100; sessionId++) {
                    String toWrite = datasetId+"\t"+sessionId+"\t"+"myFeature,0.0,1.0,2.0,3.0,4.0,5.0,6.0\n";
                    bw.write(toWrite);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = Integer.parseInt(args[0]);
        String logFilePath = args[1];
        initLogFile(logFilePath);

        MetadataService service = new MetadataService(new MetadataServiceRequestProcessor(logFilePath));
        Server server = ServerBuilder.forPort(port)
                .addService(service)
                .build();

        server.start();
        server.awaitTermination();
    }

}
