package de.hhu.bsinfo.hadronio.example.bookkeeper.benchmark;

import picocli.CommandLine;

@CommandLine.Command(
        name = "benchmark",
        description = "BookKeeper benchmark",
        showDefaultValues = true,
        separator = " ")
public class Benchmark implements Runnable {

    @CommandLine.Option(
            names = {"-c", "--connections"},
            description = "The amount of connections to use for sending/receiving.")
    private int connections = 1;

    @CommandLine.Option(
            names = {"-r", "--remote"},
            description = "The address to connect to.")
    private String remoteAddress;

    @CommandLine.Option(
            names = {"-l", "--length"},
            description = "The messages size.",
            required = true)
    private int messageSize;

    @CommandLine.Option(
            names = {"-m", "--messages"},
            description = "The amount of messages to send/receive.",
            required = true)
    private int messageCount;

    @CommandLine.Option(
            names = {"-b", "--bookkeeper-ledgers"},
            description = "The amount of ledgers to create.",
            required = true)
    private int ledgerCount;

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

    @Override
    public void run() {
        final var client = new Client(remoteAddress, ledgerCount, messageCount, messageSize, connections);
        client.run();
    }
}