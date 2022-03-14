package de.hhu.bsinfo.hadronio.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ThroughputResult {

    private final long operationCount;
    private final long operationSize;
    private final long totalData;

    private double totalTime;
    private double operationThroughput;
    private double dataThroughput;

    public ThroughputResult(final long operationCount, final long operationSize) {
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        totalData = operationCount * operationSize;
    }

    ThroughputResult(final long operationCount, final long operationSize, final long totalData, final double totalTime, final double operationThroughput, final double dataThroughput) {
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        this.totalData = totalData;
        this.totalTime = totalTime;
        this.operationThroughput = operationThroughput;
        this.dataThroughput = dataThroughput;
    }

    public long getOperationCount() {
        return operationCount;
    }

    public long getOperationSize() {
        return operationSize;
    }

    public double getOperationThroughput() {
        return operationThroughput;
    }

    public double getDataThroughput() {
        return dataThroughput;
    }

    public double getTotalTime() {
        return totalTime;
    }

    public long getTotalData() {
        return totalData;
    }

    public void setMeasuredTime(final long timeInNanos) {
        this.totalTime = timeInNanos / 1000000000d;
        operationThroughput = (double) operationCount / totalTime;
        dataThroughput = (double) totalData / totalTime;
    }

    public void writeToFile(final String fileName, final String benchmarkName, final int iteration, final int connections) throws IOException {
        final File file = new File(fileName);
        FileWriter writer;

        if (file.exists()) {
            writer = new FileWriter(fileName, true);
        } else {
            if (!file.createNewFile()) {
                throw new IOException("Unable to create file '" + fileName + "'");
            }

            writer = new FileWriter(fileName, false);
            writer.write("Benchmark,Iteration,Connections,Size,DataThroughput,OperationThroughput\n");
        }

        writer.append(benchmarkName).append(",")
                .append(String.valueOf(iteration)).append(",")
                .append(String.valueOf(connections)).append(",")
                .append(String.valueOf(getOperationSize())).append(",")
                .append(String.valueOf(getDataThroughput())).append(",")
                .append(String.valueOf(getOperationThroughput())).append("\n");

        writer.flush();
        writer.close();
    }

    @Override
    public String toString() {
        return "ThroughputResult {" +
                "\n\t" + ValueFormatter.formatValue("operationCount", operationCount) +
                ",\n\t" + ValueFormatter.formatValue("operationSize", operationSize, "Byte") +
                ",\n\t" + ValueFormatter.formatValue("totalData", totalData, "Byte") +
                ",\n\t" + ValueFormatter.formatValue("totalTime", totalTime, "s") +
                ",\n\t" + ValueFormatter.formatValue("operationThroughput", operationThroughput, "Operations/s") +
                ",\n\t" + ValueFormatter.formatValue("dataThroughput", dataThroughput, "Byte/s") +
                "\n}";
    }
}
