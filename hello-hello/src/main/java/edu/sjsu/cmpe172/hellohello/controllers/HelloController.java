package edu.sjsu.cmpe172.hellohello.controllers;

import edu.sjsu.cmpe172.hellohello.services.ZKService;
import io.grpc.ManagedChannelBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@RestController
public class HelloController {
    private final static Logger logger = Logger.getLogger(HelloController.class.getName());
    final private ZKService zkService;
    public HelloController(ZKService zkService) {
        this.zkService = zkService;
    }
    public record Info(String status, String zookeeper, String leader, String myId, String myDescription, List<String> peers) {}
    @GetMapping("/leader")
    public Info getLeader() {
        return new Info(zkService.watching ? "WATCHING" : zkService.isLeading() ? "LEADING" : "WATCHING",
                zkService.connected ? "CONNECTED" : "DISCONNECTED",
                zkService.leaderPeer,
                zkService.myName,
                zkService.myDescription,
                zkService.peers);
    }

    @PostMapping("/leader/lead")
    public String lead() {
        zkService.tryToLead(true);
        return "Trying to lead";
    }

    @PostMapping("/leader/watch")
    public String watch() {
        zkService.tryToLead(false);
        return "Watching";
    }

    public record PeersInfo(String name, String description, String hostPort, long lastTxn) {}
    @GetMapping("/peers")
    public List<PeersInfo> getPeers() {
        return zkService.peers.stream().map(peerName -> {
            try {
                var data = zkService.zk.getData("/peers/" + peerName, false, null);
                var parts = new String(data).split("\\s", 2);
                var hostPort = parts[0];
                var description = parts[1];
                long lastTxn = -100;
                try {
                    var channel = ManagedChannelBuilder.forTarget(hostPort)
                            .usePlaintext()
                            .build();
                    var stub = edu.sjsu.cmpe172.hellohello.PostReplicaServiceGrpc.newBlockingStub(channel).withDeadlineAfter(2, java.util.concurrent.TimeUnit.SECONDS);
                    var response = stub.getLastTxn(edu.sjsu.cmpe172.hellohello.HelloHello.GetLastTxnRequest.getDefaultInstance());
                    lastTxn = response.getLastTxn();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to get lastTxn from peer " + peerName + " at " + hostPort, e);
                }
                return new PeersInfo(peerName, description, hostPort, lastTxn);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to get info for peer " + peerName, e);
            }
            return new PeersInfo(peerName, "Error", "Error", -100);
        }).toList();
    }
}
