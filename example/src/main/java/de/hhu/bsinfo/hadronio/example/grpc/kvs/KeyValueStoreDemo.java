package de.hhu.bsinfo.hadronio.example.grpc.kvs;

import de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb.YcsbRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import site.ycsb.measurements.Measurements;

import java.net.InetSocketAddress;
import java.nio.file.Path;

@CommandLine.Command(
        name = "kvs",
        description = "Example application, that implements a key value store.",
        showDefaultValues = true,
        separator = " ")
public class KeyValueStoreDemo implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueStoreDemo.class);
    private static final int DEFAULT_SERVER_PORT = 2998;

    @CommandLine.Option(
            names = {"-s", "--server"},
            description = "Run this instance in server mode.")
    private boolean isServer = false;

    @CommandLine.Option(
            names = {"-a", "--address"},
            description = "The address to bind to.")
    private InetSocketAddress bindAddress = null;

    @CommandLine.Option(
            names = {"-r", "--remote"},
            description = "The address to connect to.",
            split = ",")
    private InetSocketAddress[] remoteAddress;

    @CommandLine.Option(
            names = {"-b", "--benchmark"},
            description = "Run the YCSB client."
    )
    private boolean benchmark = false;

    @CommandLine.Option(
            names = {"-w", "--workload"},
            description = "The workload file for the YCSB client.")
    private Path workload;

    @CommandLine.Option(
            names = {"-p", "--phase"},
            description = "The benchmark phase to execute (LOAD/RUN).")
    private YcsbRunner.Phase phase;

    @CommandLine.Option(
            names = {"-t", "--threads"},
            description = "The amount of threads to use for the YCSB client.")
    private int threads = 1;


    @CommandLine.Option(
            names = {"-c", "--connections"},
            description = "The amount of clients, that will connect to the server (only relevant for YCSB).")
    private int connections = 1;

    @CommandLine.Option(
            names = {"-l", "--live-status"},
            description = "Enable YCSB status reports during the benchmark"
    )
    private boolean status = false;

    @CommandLine.Option(
            names = {"-z", "--measurement-type"},
            description = "Set the measurement type (HDRHISTOGRAM, HISTORGRAM, TIMESERIES)"
    )
    private Measurements.MeasurementType measurementType = Measurements.MeasurementType.HDRHISTOGRAM;

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Path to the result file, to which the CSV data shall be written.")
    private String resultFileName = "";

    @CommandLine.Option(
            names = {"-n", "--name"},
            description = "Benchmark name to use, when writing the result to a file.")
    private String benchmarkName = "";

    @CommandLine.Option(
            names = {"-i", "--iteration"},
            description = "Iteration number to use, when writing the result to a file.")
    private int benchmarkIteration = 0;

    @CommandLine.Option(
            names = {"-m", "--record-size"},
            description = "Iteration number to use, when writing the result to a file.")
    private int recordSize = 0;

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        if (bindAddress == null) {
            bindAddress = isServer ? new InetSocketAddress(DEFAULT_SERVER_PORT) : null;
        } else {
            bindAddress = isServer ? bindAddress : new InetSocketAddress(bindAddress.getAddress(), 0);
        }

        Runnable runnable;
        if (isServer) {
            runnable = new Server(bindAddress, connections);
        } else if (benchmark) {
            if (workload == null) {
                LOGGER.error("Please specify the YCSB properties file");
                return;
            }

            if (phase == null) {
                LOGGER.error("Please specify the YCSB phase to execute");
                return;
            }

            runnable = new YcsbRunner(remoteAddress, workload, phase, measurementType, threads, status, resultFileName, benchmarkName, benchmarkIteration, recordSize);
        } else {
            runnable = new Shell(remoteAddress);
        }

        runnable.run();
    }
}
