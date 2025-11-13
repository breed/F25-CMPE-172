package edu.sjsu.cmpe172.hellohello.services;

import jakarta.annotation.PostConstruct;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


@Service
public class ZKService {
    private static final Logger logger = Logger.getLogger(ZKService.class.getName());
    public boolean connected = false;
    public String leaderPeer = null;
    public long leaderZxid = -1;
    public String myName = null;
    public boolean watching = false;
    public ZooKeeper zk;
    public List<String> peers = List.of();
    final public String myDescription;
    public List<String> replicas;
    private int replicasVersion = -1;

    ZKService(@Value("${zkConnectString}") String connectString, @Value("${grpc.server.port}") int grpcPort,
              @Value("${myDescription}") String myDescription, @Value("${serverId}") String serverId)
            throws IOException, InterruptedException, KeeperException {
        this.myDescription = myDescription;
        this.myName = serverId;
        zk = new ZooKeeper(connectString, 3000, event -> {
            switch (event.getState()) {
                case SyncConnected -> connected = true;
                case Disconnected -> connected = false;
                case Expired -> {
                    logger.log(Level.SEVERE, "ZooKeeper session expired");
                    System.exit(2);
                }
                case AuthFailed -> {
                    logger.log(Level.SEVERE, "ZooKeeper authentication failed");
                    System.exit(2);
                }
            }
        });
        // little trick to get the host IP address that other peers can use to connect to us
        // it doesn't matter which port we use here, we just want to figure out the right network interface
        try (var dsock = new DatagramSocket()) {
            String zkhost = connectString.substring(0,connectString.lastIndexOf(':'));
            dsock.connect(InetAddress.getByName(zkhost), 65530);
            String hostAddress = dsock.getLocalAddress().getHostAddress();
            if (hostAddress.contains(":")) {
                // why did they make IPv6 addresses contain colons... anyway, we need to wrap it in brackets
                hostAddress = "[" + hostAddress + "]";
            }
            zk.create("/peers/" + myName, (hostAddress + ":" + grpcPort + "\n" + myDescription).getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
    }

    @PostConstruct
    public void postInit() throws InterruptedException, KeeperException {
        zk.addWatch("/leader", leaderWatch, AddWatchMode.PERSISTENT);
        zk.addWatch("/replicas", event -> getReplicas(), AddWatchMode.PERSISTENT);
        zk.addWatch("/peers", peerWatch, AddWatchMode.PERSISTENT);
        getChildren();
        getReplicas();
        getLeader();
    }

    private void getReplicas() {
        zk.getData("/replicas", false, (rc, path, ctx, data, stat) -> {
            if (rc == KeeperException.Code.NONODE.intValue()) {
                logger.log(Level.WARNING, "/replicas does not exist");
                replicas = List.of();
                return;
            } else if (rc != KeeperException.Code.OK.intValue()) {
                logger.log(Level.SEVERE, "Error getting data of /replicas: " + KeeperException.Code.get(rc));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // i really hate the interrupted exception!
                }
                getReplicas();
                return;
            }
            replicas = List.of(new String(data).split(","));
            replicasVersion = stat.getVersion();
            logger.log(Level.INFO, "Current replicas: " + replicas);
        }, null);
    }

    public void setReplicas(String commaSeparatedReplicas) throws InterruptedException, KeeperException {
        if (!myName.equals(leaderPeer)) {
            logger.log(Level.SEVERE, "Cannot set /replicas because we don't know its version");
            throw new IllegalStateException("Not the leader");
        }
        zk.setData("/replicas", commaSeparatedReplicas.getBytes(), replicasVersion);
    }

    private void getLeader() {
        zk.getData("/leader", false, (rc, path, ctx, data, stat) -> {
            if (rc == KeeperException.Code.NONODE.intValue()) {
                logger.log(Level.INFO, "/leader does not exist");
                leaderPeer = null;
                if (!watching) {
                    if (!replicas.contains(myName)) {
                        logger.log(Level.WARNING, "Not in replicas list, cannot become leader");
                        return;
                    }
                    logger.log(Level.INFO, "trying to become leader");
                    zk.create("/leader", myName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, (rc2, path2, ctx2, name) -> {
                        if (rc2 == KeeperException.Code.OK.intValue()) {
                            // the watch should trigger and we will find out that we are the leader
                            logger.log(Level.INFO, "Became leader: " + leaderPeer);
                        } else if (rc2 == KeeperException.Code.NODEEXISTS.intValue()) {
                            logger.log(Level.INFO, "Another peer became leader before us");
                            getLeader();
                        } else {
                            logger.log(Level.SEVERE, "Error creating /leader: " + KeeperException.Code.get(rc2));
                        }
                    }, null);
                }
                return;
            } if (rc != KeeperException.Code.OK.intValue()) {
                logger.log(Level.SEVERE, "Error getting data of /leader: " + KeeperException.Code.get(rc));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // i really hate the interrupted exception!
                }
                getLeader();
                return;
            }
            leaderPeer = new String(data);
            leaderZxid = stat.getMzxid();
            logger.log(Level.INFO, "Current leader: " + leaderPeer);
        }, null);
    }
    final Watcher leaderWatch = event -> {
        logger.log(Level.INFO, "Leader node changed: " + event);
        switch (event.getType()) {
            case NodeCreated, NodeDeleted, NodeDataChanged -> {
                getLeader();
            }
        }
    };
    final Watcher peerWatch = event -> {
        logger.log(Level.INFO, "Peer nodes changed: " + event);
        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            getChildren();
        }
    };

    private void getChildren() {
        zk.getChildren("/peers", false, (rc, path, ctx, childs) -> {
            if (rc != KeeperException.Code.OK.intValue()) {
                logger.log(Level.SEVERE, "Error getting children of /peers: " + KeeperException.Code.get(rc));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // i really hate the interrupted exception!
                }
                getChildren();
                return;
            }
            peers = childs;
            logger.log(Level.INFO, "Current peers: " + childs);
        }, null);
    }

    private void giveUpLeadership() {
        if (!myName.equals(leaderPeer)) {
            logger.log(Level.INFO, "Not the leader, cannot give up leadership");
            return;
        }
        zk.delete("/leader", -1, (rc, path, ctx) -> {
            if (rc == KeeperException.Code.OK.intValue()) {
                logger.log(Level.INFO, "Gave up leadership");
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                logger.log(Level.INFO, "No /leader node to give up");
            } else {
                logger.log(Level.SEVERE, "Error deleting /leader: " + KeeperException.Code.get(rc));
                giveUpLeadership();
            }
        }, null);
    }
    public void tryToLead(boolean tryingToLead) {
        watching = !tryingToLead;
        if (!tryingToLead && leaderPeer.equals(myName)) {
            giveUpLeadership();
        }
    }

    public boolean isLeading() {
        return myName.equals(leaderPeer);
    }
}