package edu.sjsu.cmpe172.hellohello.services;

import edu.sjsu.cmpe172.hellohello.PostReplicaServiceGrpc;
import io.grpc.ManagedChannelBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class InitializerCommandRunner implements CommandLineRunner {
    private static final Logger logger = Logger.getLogger(InitializerCommandRunner.class.getName());
    private final boolean initialize;
    private final String breakTheWorld;
    private final ZKService zkService;

    public InitializerCommandRunner(@Value("${initialize:false}") boolean initialize, @Value("@{breakTheWorld:}") String breakTheWorld, ZKService zkService) {
        this.initialize = initialize;
        this.breakTheWorld = breakTheWorld;
        this.zkService = zkService;
    }

    @Override
    public void run(String... args) throws Exception {
        logger.log(Level.INFO, "Running InitializerCommandRunner with initialize=" + initialize + " and breakTheWorld=" + breakTheWorld);

        String mostUpToDatePeer = null;
        long mostUpToDateTxn = Long.MIN_VALUE;

        for (var peer : zkService.peers) {
            var data = zkService.zk.getData("/peers/" + peer, false, null);
            // the data will have host:port first, but there might be other info after a whitespace
            var hostPort = new String(data).split("\\s")[0];
            // make a gRPC call to getLastTxn
            var channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build();
            var stub = PostReplicaServiceGrpc.newBlockingStub(channel);
            try {
                var response = stub.getLastTxn(edu.sjsu.cmpe172.hellohello.HelloHello.GetLastTxnRequest.newBuilder().build());
                logger.log(Level.INFO, "Peer " + peer + " at " + hostPort + " has lastTxn: " + response.getLastTxn());
                if (response.getLastTxn() > mostUpToDateTxn) {
                    mostUpToDateTxn = response.getLastTxn();
                    mostUpToDatePeer = peer;
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to get lastTxn from peer " + peer + " at " + hostPort, e);
                continue;
            }
        }
        try {
            var replicas = new String(zkService.zk.getData("/replicas", false, null));
            System.out.println("Current replicas are " + replicas);
            if (initialize) {
                if (breakTheWorld != null) {
                    logger.log(Level.WARNING, "Not initializing /replicas because it already exists.");
                    return;
                }
                var rand = new Random();
                var a = rand.nextInt(6) + 3;
                var b = rand.nextInt(6) + 3;
                System.out.printf("You have chosen to break the world with %s. What is %d + %d?", replicas, a, b);
                var answer = System.console().readLine();
                if (Integer.parseInt(answer.trim()) == a + b) {
                    zkService.zk.setData("/replicas", breakTheWorld.getBytes(), -1);
                    logger.log(Level.INFO, "/replicas has been set to " + breakTheWorld);
                } else {
                    System.out.println("Incorrect answer. Aborting.");
                }
            }
        } catch (KeeperException.NoNodeException e) {
            logger.log(Level.INFO, "/replicas does not exist.");
            if (initialize) {
                if (mostUpToDateTxn > -1) {
                    var data = zkService.zk.getData("/peers/" + mostUpToDatePeer, false, null);
                    zkService.zk.create("/replicas", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    logger.log(Level.INFO, "Initialized /replicas with data from most up-to-date peer " + mostUpToDatePeer);
                } else {
                    if (mostUpToDatePeer != null) {
                        zkService.zk.create("/replicas", mostUpToDatePeer.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        System.out.println("Initialized /replicas with " + mostUpToDatePeer);
                    } else {
                        System.out.println("No peers found to initialize /replicas.");
                    }
                }
            }
        }
    }
}
