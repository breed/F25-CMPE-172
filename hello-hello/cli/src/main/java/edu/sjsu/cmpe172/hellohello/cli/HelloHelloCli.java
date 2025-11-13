package edu.sjsu.cmpe172.hellohello.cli;

import io.grpc.ManagedChannelBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Random;
import java.util.concurrent.Callable;

import edu.sjsu.cmpe172.hellohello.PostReplicaServiceGrpc;

@Command(name = "hellohello-cli", mixinStandardHelpOptions = true, description = "CLI for HelloHello service")
public class HelloHelloCli implements Callable<Integer> {
    @CommandLine.Option(
            names = {"--zk-connection-string"},
            description = "Connection string to the ZooKeeper for HelloHello service",
            required = true
    )
    String connectionString;
    @CommandLine.Option(
            names = {"--initialize"},
            description = "Initialize the HelloHello service by creating /replicas in ZooKeeper",
            defaultValue = "false"
    )
    boolean initialize;

    @CommandLine.Option(
            names = {"--break-the-world"},
            description = "Rather than sanity check the setting of /replicas, force it to the specified value. Must be used with --initialize."
    )
    String replicas;

    public static void main(String[] args) {
        var cmdLine = new CommandLine(new HelloHelloCli());
        System.exit(cmdLine.execute(args));
    }

    @Override
    public Integer call() throws Exception {
        String mostUpToDatePeer = null;
        long mostUpToDateTxn = Long.MIN_VALUE;
        try (var zk = new ZooKeeper(connectionString, 3000, null)) {
            for (var peer : zk.getChildren("/peers", false)) {
                var data = zk.getData("/peers/" + peer, false, null);
                // the data will have host:port first, but there might be other info after a whitespace
                var hostPort = new String(data).split("\\s")[0];
                // make a gRPC call to getLastTxn
                var channel = ManagedChannelBuilder.forTarget(hostPort)
                        .usePlaintext()
                        .build();
                var stub = PostReplicaServiceGrpc.newBlockingStub(channel);
                var response = stub.getLastTxn(edu.sjsu.cmpe172.hellohello.HelloHello.GetLastTxnRequest.newBuilder().build());
                System.out.println("Peer " + peer + " at " + hostPort + " has lastTxn: " + response.getLastTxn());
                if (response.getLastTxn() > mostUpToDateTxn) {
                    mostUpToDateTxn = response.getLastTxn();
                    mostUpToDatePeer = peer;
                }
            }
            try {
                var replicas = new String(zk.getData("/replicas", false, null));
                System.out.println("Current replicas are " + replicas);
                if (initialize) {
                    if (this.replicas == null) {
                        System.out.println("Not initializing /replicas because it already exists.");
                        return 1;
                    }
                    var rand = new Random();
                    var a = rand.nextInt(6) + 3;
                    var b = rand.nextInt(6) + 3;
                    System.out.printf("You have chosen to break the world with %s. What is %d + %d?", replicas, a, b);
                    var answer = System.console().readLine();
                    if (Integer.parseInt(answer.trim()) == a + b) {
                        zk.setData("/replicas", this.replicas.getBytes(), -1);
                        System.out.println("/replicas has been set to " + this.replicas);
                        return 0;
                    } else {
                        System.out.println("Incorrect answer. Aborting.");
                        return 2;
                    }
                }
            } catch (KeeperException.NoNodeException e) {
                System.out.println("/replicas does not exist.");
                if (initialize) {
                    if (mostUpToDateTxn > -1) {
                        var data = zk.getData("/peers/" + mostUpToDatePeer, false, null);
                        zk.create("/replicas", data, null, null);
                        System.out.println("Initialized /replicas with data from most up-to-date peer " + mostUpToDatePeer);
                    } else {
                        if (replicas != null) {
                            zk.create("/replicas", this.replicas.getBytes(), null, null);
                            System.out.println("Initialized /replicas with provided replicas: " + this.replicas);
                            return 0;
                        } else if (mostUpToDatePeer != null) {
                            zk.create("/replicas", mostUpToDatePeer.getBytes(), null, null);
                            System.out.println("Initialized /replicas with " + mostUpToDatePeer);
                            return 0;
                        } else {
                            System.out.println("No peers found to initialize /replicas.");
                            return 2;
                        }
                    }
                }
            }
            return 0;
        }
    }
}
