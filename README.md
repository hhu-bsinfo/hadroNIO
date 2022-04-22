# hadroNIO

Transparent acceleration for Java NIO applications via [UCX](https://openucx.org).

<p align="center">
<img src=media/logo.svg width=400>
</p>

<p align="center">
  <a href="https://travis-ci.com/github/hhu-bsinfo/hadroNIO"><img src="https://www.travis-ci.com/hhu-bsinfo/hadroNIO.svg?branch=main"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/java-8+-blue.svg"></a>
  <a href="https://openucx.org/"><img src="https://img.shields.io/badge/ucx-1.12.1-blue.svg"></a>
  <a href="https://github.com/hhu-bsinfo/hadroNIO/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-GPLv3-orange.svg"></a>
</p>

## Introduction

Java NIO is the standard for modern network development on the Java platform for many years now. With its elegant API for asynchronous communication, it empowers application developers to handle several connections with just a single thread, while still being flexible to scale with large thread counts. Additionally, it supports blocking communication, resembling the traditional Java socket API.  
However, since the NIO implementation relies on classic sockets, applications are limited to using Ethernet for communication.

[Unified Communication X](https://openucx.org) (UCX) is a native framework, aiming to provide a unified API for multiple transport types. The UCX API offers several forms of communication, such as tagged messaging, active messaging, streaming or RDMA. Application developers do not need to target a specific network interconnect, since UCX automatically scans the system for available transports and chooses the fastest one (e.g. Ethernet or InfiniBand).  
Its Java-binding called JUCX (based on JNI) makes it also suitable for Java applications.

With **hadroNIO**, we aim at combining these two frameworks, by providing a new NIO implementation, which leverages UCX to send and receive network traffic. Thus, hadroNIO can transparently accelerate existing Java NIO applications, using the fastest network interconnect available in a given environment.

This is a research project by the [Operating Systems group](https://www.cs.hhu.de/en/research-groups/operating-systems.html) at the *Heinrich Heine University Düsseldorf*.

<p align="center">
  <a href="https://www.uni-duesseldorf.de/home/en/home.html"><img src="media/hhu.svg" width=300></a>
</p>

## Build instructions

hadroNIO is compatible with all Java versions, starting from Java 8.

Execute the following commands to clone this repository and build a portable JAR-file, containing hadroNIO and all its dependencies:

```shell
git clone https://github.com/hhu-bsinfo/hadroNIO.git
cd hadroNIO/
./gradlew shadowJar
```

The JAR-file should now be located at `build/provider/libs/hadronio-0.3.2-SNAPSHOT-all.jar`.

### Known issues

 - Building hadroNIO with a Java version higher than 8, but then running it with Java 8 JVM results in a `java.lang.NoSuchMethodError`, regarding the class `java.nio.ByteBuffer`. This happens, because the `ByteBuffer` overrides methods of its super class `Buffer` in Java 9+, while it relies on the implementations provided by `Buffer` in Java 8. If you come across this error, make sure to both build an run hadroNIO using Java 8, or use a newer version of Java altogether.

## Run instructions

To run hadroNIO, **UCX 1.12.1** needs to be installed on your system. See the [OpenUCX GitHub Repository](https://github.com/openucx/ucx) for information on how to build and install UCX.

To accelerate an existing Java application (e.g. `application.jar`), the hadroNIO JAR-file needs to be included in the classpath. Additionally, the property `java.nio.channels.spi.SelectorProvider` must be set to `de.hhu.bsinfo.hadronio.HadronioProvider`:
```shell
java -cp path/to/hadronio-0.3.2-SNAPSHOT-all.jar -Djava.nio.channels.spi.SelectorProvider=de.hhu.bsinfo.hadronio.HadronioProvider -jar application.jar
```

### Enable logging

hadroNIO uses [SLF4J](http://www.slf4j.org/) for logging. To see any log output, you need to supply an appropriate logging framework, that supports SLF4J (we recommend [Log4j 2](https://logging.apache.org/log4j/2.x/) with the [log4j-slf4j-impl](https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-slf4j-impl) module). Either load the framework's JAR file into the classpath, when starting your application, or add the framework as a build dependency to your application. For Gradle, this can be done in the following way:

```groovy
dependencies {
    ...
    implementation 'org.apache.logging.log4j:log4j-slf4j-impl:2.17.2'
    ...
}
```

Additionally, you need to configure SLF4J to enable logging output for `de.hhu.bsinfo`. This can be achieved, by including a file called `log4j2.xml` in your project's resources. Our recommended configuration looks like this:

```xml
<Configuration status="warn">
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%highlight{[%d{HH:mm:ss.SSS}][%t{4}][%level{WARN=WRN, DEBUG=DBG, ERROR=ERR, TRACE=TRC, INFO=INF, FATAL=FAT}][%c{1}] %msg%n}{FATAL=red, ERROR=red, WARN=yellow, INFO=blue, DEBUG=green, TRACE=white}"/>
    </Console>
    
    <Async name="ConsoleAsync" bufferSize="500">
      <AppenderRef ref="Console"/>
    </Async>
  </Appenders>
  
  <Loggers>
    <Root level="error">
      <AppenderRef ref="ConsoleAsync"/>
    </Root>
    
    <Logger name="de.hhu.bsinfo" level="info" additivity="false">
      <AppenderRef ref="ConsoleAsync" />
    </Logger>
  </Loggers>
</Configuration>
```

You should now see log output from hadroNIO in your terminal. If everything is configured correctly, the first line of log output should look like the following:
```console
[13:55:19.021][main][INF][HadronioProvider] Initializing HadronioProvider
```

To enable more detailed log messages, just set the log level to `debug`. However, this will drastically decrease performance and is not recommended for normal usage.

## Test instructions

This repository contains a test application with several commands, which includes hadroNIO as dependency and is automatically accelerated, without passing parameters to the `java` command. Run the following command inside the hadroNIO project directory, to build this application:
```shell
./gradlew installDist
```

### Tests using blocking socket channels

These commands use blocking socket channels for communication.

#### Counter

The counter command starts a simple test, which sends an increasing number to the remote side, while also receiving an increasing from the remote side.

Start a server:
```shell
./build/example/install/hadronio/bin/hadronio blocking counter --server
```

Start a client:
```shell
./build/example/install/hadronio/bin/hadronio blocking counter --remote <server address>
```

#### Benchmark

The benchmark command can be used for quick unidirectional performance tests with two nodes. The subcommand `throughput` starts a throughput benchmark, while the subcommand `latency` measures round trip times.

Start a server:
```shell
./build/example/install/hadronio/bin/hadronio blocking benchmark throughput --server
```
Start a client:
```shell
./build/example/install/hadronio/bin/hadronio blocking benchmark throughput --remote <server address>
```

### Tests using netty

These commands use [netty](https://netty.io/) and thus non-blocking socket channels for communication.

#### Hello

A simple test, where the client sends a short message and terminates, once it has received an answer from the server.

Start a server:
```shell
./build/example/install/hadronio/bin/hadronio netty hello --server
```
Start a client:
```shell
./build/example/install/hadronio/bin/hadronio netty hello --remote <server address>
```

#### Echo

This command implements the echo protocol, meaning that the server always answers with a copy of everything it receives.  
The client reads lines from standard input, sends each line to the server and waits for an answer.

Start a server:
```shell
./build/example/install/hadronio/bin/hadronio netty echo --server
```
Start a client:
```shell
./build/example/install/hadronio/bin/hadronio netty echo --remote <server address>
```

#### Benchmark

This is the equivalent to the blocking benchmark command.

Start a server:
```shell
./build/example/install/hadronio/bin/hadronio netty benchmark throughput --server
```
Start a client:
```shell
./build/example/install/hadronio/bin/hadronio netty benchmark throughput --remote <server address>
```

### Parameters

The test application can be configured using the following parameters:

 - `-s`, `--server`: Start a server instance, waiting for a client to connect.
 - `-r`, `--remote`: The remote address to connect to.
 - `-a`, `--address`: The local address to bind to (default: `0.0.0.0:2998`)
 - `-m`, `--message`: The number of messages to send/receive.
 - `-l`, `--length`: The message size (only valid for benchmark).
 - `-t`, `--threshold`: The amount of messages to send, before flushing the channel (only available in throughput benchmarks).
 - `-c`, `--connections`: The amount of connections to use (only available in netty benchmarks).

To run the test application without hadroNIO, set the environment variable `DISABLE_HADRONIO` to `true`.

## Configuration

It is possible to configure hadroNIO via system properties. These can be set by supplying parameters such as `-D<property>=<value>` to the java command, when running your application.  
The following properties are supported:

- `de.hhu.bsinfo.hadronio.Configuration.PROVIDER_CLASS`: Set the UCX provider class (Default: `de.hhu.bsinfo.hadronio.jucx.JucxProvider`). hadroNIO can support different Java bindings for UCX. However, at the moment only `JUCX` is supported, and this value should not be changed.
- `de.hhu.bsinfo.hadronio.Configuration.SEND_BUFFER_LENGTH`: Set the size of the send ring buffer in byte (Default: `8388608`).
- `de.hhu.bsinfo.hadronio.Configuration.RECEIVE_BUFFER_LENGTH`: Set the size of the receive ring buffer in byte (Default: `8388608`).
- `de.hhu.bsinfo.hadronio.Configuration.BUFFER_SLICE_LENGTH`: Set the size of the buffer slices used for sending/receiving data (Default: `65536`). This value can have a huge performance impact, since it determines the maximum amount of data, that is send/received at once per channel.
- `de.hhu.bsinfo.hadronio.Configuration.FLUSH_INTERVAL_SIZE`: Set the interval in which channels should be flushed (Default: `1024`). Every time, the set amount of messages has been sent, the channel will stop signalling `OP_WRITE`, until it has received an automatic acknowledgment message from the receiving side. This is done to prevent a receiver from being overloaded by too many messages. The default value did work fine in our tests, and there should be no need to alter it.

## Include in other projects

It is possible to use hadroNIO in other Gradle projects. The latest releases are available from the GitHub Package Registry.
To include hadroNIO into your project, use the following code in your `build.gradle`:

```groovy
repositories {
    maven {
        name = "GitHubPackages hadroNIO"
        url = "https://maven.pkg.github.com/hhu-bsinfo/hadronio"
        credentials {
            username = project.findProperty("gpr.user")
            password = project.findProperty("gpr.token")
        }
    }
}

dependencies {
    implementation 'de.hhu.bsinfo:hadronio:0.3.2'
}
```

Use a file called `gradle.properties` to set `gpr.user` to your GitHub username and `gpr.token` to a Personal Access Token with `read:packages` enabled. See the [GitHub Docs](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-gradle-registry) for more information on the GitHub Package Registry.

To enable hadroNIO from within your application, use the following code:

```java
System.setProperty("java.nio.channels.spi.SelectorProvider", "de.hhu.bsinfo.hadronio.HadronioProvider");
```

The configuration values can be set with similary calls. This way, hadroNIO will be included in your project and properties do not have to be set manually, each time the application is started.

## Architecture

To transparently accelerate existing NIO applications, hadroNIO needs to fully substitute the involved classes, including `SocketChannel`, `ServerSocketChannel`, `Selector` and `SelectionKey`.
The Java platform provides a comfortable way of exchanging the default NIO implementation through a class called `SelectorProvider`.  This class offers methods to create instances of the different NIO components (e.g. `SocketChannel` or `Selector`). The provider class to use can be set via the system property `java.nio.channels.spi.SelectorProvider` (see [Run instructions](#run-instructions)).

<p align="center">
<img src=media/architecture.svg width=500>
</p>

### Buffer management for writing

Buffers are managed differently in UCX and NIO: In the default NIO implementation, calling `write()` will copy the source buffer's content into the underlying socket's buffer and return. Even though the actual process of sending the data is then performed asynchronously, the source buffer may be reused and altered by the application.
UCX's behaviour differs from that by not allowing the source buffer to be modified until the request is completed.
We address this by introducing an intermediate buffer to hadroNIO's `SocketChannel` implementation. In its `write()` method, the source buffer's content is copied into the intermediate buffer and all UCX send requests will only operate on the  copied data.
Since we want to be able to handle multiple active send requests, a simple yet thread-safe memory management is needed to manage the space inside the intermediate buffer. To achieve this, the buffer is implemented as a ring buffer, based on [Agrona's OneToOneRingBuffer](https://github.com/real-logic/Agrona).

The full write mechanism can be divided into the following steps:

 1. Allocate the needed amount of space inside the intermediate buffer.
 2. Copy the source buffer's content into the newly allocated space.
 3. Issue a send request via UCX.
 4. Return to the application. The source buffer may now be reused and the actual process of sending the data to a remote receiver is performed asynchronously.
 5. Once the request has been completed by UCX, a callback is invoked.
 6. The space inside the intermediate buffer is not needed anymore and is freed by the callback routine.

<p align="center">
    <img src=media/send.svg width=500>
</p>

### Buffer management for reading

In the traditional NIO implementation, all received data is first being stored in the underlying socket's internal buffer and the `read()` method copies this data into the application's target buffer.
A similar technique is applied in hadroNIO's `read()` implementation: Equivalent to the `write()` method, an intermediate buffer is used to store asynchronously received data and `read()` just needs copy this data.
To issue receive requests to UCX, the method `fillReceiveBuffer()` is introduced to the `SocketChannel` class. This method allocates several slices of the same length inside the intermediate buffer and creates a receive request for each of these slices.
This implies, that send requests, issued by `write()`, may not be larger than the slices created by `fillReceiveBuffer()`. To accommodate for that, `write()` divides larger buffers into multiple smaller send requests, that fit into the slices inside the remote's receive buffer.
To ensure that hadroNIO never runs out of active receive requests, `fillReceiveBuffer()` is called once a connection has been established, and afterwards inside each selection operation.

The full read mechanism can be divided into the following steps:

 1. Slices inside the intermediate receive buffer are allocated by `fillReceiveBuffer()`.
 2. A receive request is issued for each of the newly allocated slices.
 3. Once a request has been completed by UCX, a callback is invoked.
 4. The callback routine notifies the socket channel, that a new buffer slice has been filled with data. The channel keeps an internal counter of how many of the allocated slices contain valid data.
 5. When the application calls `read()`, the content of a buffer slice is copied into the destination buffer. If a slice has been read fully, the allocated space is freed and reused the next time `fillReceiveBuffer()` is called.

<p align="center">
    <img src=media/receive.svg width=500>
</p>

### Blocking vs. non-blocking socket channels

To actually send or receive data with UCX, the appropriate worker instance needs to be progressed.
In non-blocking mode, this is done inside the associated selector's `select()` method. However, in blocking mode no selector is involved, which means that the worker has to be progressed elsewhere.
For `write()`, this is done right after the send request for the last buffer slice has been issued, implying that in contrary to non-blocking mode, the data to send has already been processed by UCX, once `write()` returns. Naturally, this approach favours latency over throughput.
For `read()`, the worker is progressed and `fillReceiveBuffer()` called every time there are no slices left to be read from the intermediate receive buffer.

## Evaluation

We compared hadroNIO to `IP over Infiniband`, as well as directly programming with `JUCX`. For the evaluation, we used the [Observatory benchmark](https://github.com/hhu-bsinfo/observatory), which provides bindings for Java NIO and `JUCX`. We measured the throughput and round trip times in a unidirectional scenario, using two identical nodes with the following setup:

 Component | Used 
 --------- | --------------------------------------------------------------
 CPU       | Intel(R) Xeon(R) CPU E5-1650 v4 (6 Cores/12 Threads @3.60 GHz)
 RAM       | 64 GB DDR4 @2400 MHz
 NIC       | Mellanox Technologies MT27500 Family [ConnectX-3] (56 GBit/s)
 OS        | CentOS 8.1-1.1911 with Linux kernel 4.18.0-151
 JDK       | OpenJDK 1.8.0_265
 UCX       | 1.10.0 stable

For hadroNIO, we used the following configuration values (see [Configuration](#configuration)):

 Property              | Value
 --------------------- | -------
 SEND_BUFFER_LENGTH    | 8 MiB
 RECEIVE_BUFFER_LENGTH | 8 MiB
 BUFFER_SLICE_LENGTH   | 64 KiB
 FLUSH_INTERVAL_SIZE   | 1024

The throughput results are depicted as line plots with the left y-axis showing the operation rate in million operations per second (Mop/s) and the right axis showing the data throughput in GB/s. For the latency results, the left y-axis shows the latency in μs and the right y-axis the operation throughput in Mop/s.
The dotted lines always depict the operation throughput, while the solid lines represent either the throughput in GB/s or the latency in μs, depending on the benchmark.
Each benchmark run was executed five times and the average values are used to depict the graph, while the error bars visualize the standard deviation.

### Blocking Throughput

<p align="center">
    <img src=media/blocking-tp.svg width=500><br>
    Throughput measurements using blocking socket channels
</p>

We can see, that hadroNIO performs better than IPoIB using blocking socket channels, reaching 4.5 GB/s at a message size of 8 KiB. Starting with 16 KiB, hadroNIO's throughput drops to around 2.5 GB/s and slowly increases from there, eventually outperforming IPoIB again at 128 KiB.  
The performance drop can be explained by the different ways UCX handles small and large message sizes: Up to 8 KiB, send requests are typically processed instantly, while with larger buffers, asynchronous request processing is used, which should, in theory, be beneficial for data throughput.
However, hadroNIO's `write()` implementation waits until UCX has processed all requests associated with the current operation, when blocking mode is configured. This results in only a single asynchronous request being processed at a time for buffers smaller than the configured slice length, limiting throughput.
This problem might be solved by using a separate thread for polling the UCX worker. However, this feature is currently in an experimental state (see [Configuration](#configuration)).

### Non-Blocking Throughput

<p align="center">
    <img src=media/non-blocking-tp.svg width=500><br>
    Throughput measurements using non-blocking socket channels
</p>

Compared to using blocking socket channels, the operation throughput for small messages decreases, when using non-blocking socket channels, due to the overhead caused by the selector's logic. However, hadroNIO still manages to to process more operations per second than IPoIB (ca. 850 Kop/s vs ca 620 Kop/s using 4 byte buffers).
With larger buffers, hadroNIO's data throughput increases rapidly, reaching 6 GB/s at 16 KiB. In contrast to using blocking socket channels, there is no performance drop from 8 KiB to 16 KiB and the the throughput stays stable at 6 GB/s going further, almost matching the maximum throughput of 6.2 GB/s, reached by JUCX.

### Blocking Latency (RTT)

<p align="center">
    <img src=media/blocking-avg-latency.svg width=500><br>
    Latency measurements using blocking socket channels
</p>

Compared to directly programming with JUCX, hadroNIO introduces only a small latency overhead. Up to 64 byte buffer sizes, JUCX yields average round trip times of 2.6 μs, while hadroNIO delivers latencies of 3.1 μs, indicating that hadroNIO's buffer management has an overhead of just 500 ns.
Contrary, IPoIB provides results more than 5 times worse with latencies over 17 μs and an operation rate of 58 Kop/s vs hadroNIO's 320 Kop/s.
Naturally, with growing payloads copying data between the application and hadroNIO's internal buffers takes more time, but even at 1 MiB the difference is only around 60 μs, with JUCX needing just over 340 μs for a full round trip iteration and hadroNIO around 405 μs.

### Non-Blocking Latency (RTT)

<p align="center">
    <img src=media/non-blocking-avg-latency.svg width=500><br>
    Latency measurements using non-blocking socket channels
</p>

As expected, both hadroNIO and IPoIB yield higher latencies using non-blocking socket channels. Nevertheless, hadroNIO manages to yield round trip times as low as 5 μs and staying within single digit microsecond latencies up to 2 KiB buffer sizes.
With 16 to 19 μs, IPoIB's latency results in that range are more than 3 times as high. This is also reflected by the operation throughput, with hadroNIO reaching 200 Kop/s and IPoIB maxing out at around 60 Kop/s.

## Publications

- *hadroNIO: Accelerating Java NIO via UCX*, Fabian Ruhland, Filip Krakowski, Michael Schöttner; appeared in: Proceedings of the IEEE International Symposium on Parallel and Distributed Computing ([ISPDC](https://ispdc2021.utcluj.ro/), [IEEE Xplore](https://ieeexplore.ieee.org/document/9521601)), Cluj-Napoca, Romania, 2021.