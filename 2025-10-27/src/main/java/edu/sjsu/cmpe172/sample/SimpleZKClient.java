package edu.sjsu.cmpe172.sample;

import org.apache.zookeeper.*;
import picocli.CommandLine;

import java.io.IOException;

@CommandLine.Command(name = "SimpleZKClient")
public class SimpleZKClient implements Runnable {
    @CommandLine.Parameters
    String connectString;

    @CommandLine.Option(names = "--description", defaultValue = "anonymous")
    String description;
    private ZooKeeper zk;
    private String name;

    public static void main(String[] args) {
        System.exit(new CommandLine(new SimpleZKClient()).execute(args));
    }

    Watcher childrenWatcher = e -> {
        if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            System.out.println("Children changed! Children are now:");
            try {
                var children = zk.getChildren("/", this.childrenWatcher);
                /* option 1
                if (!children.contains("boss")) {
                    System.out.println(children);
                    try {
                        zk.create("/boss", name.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    } catch (KeeperException.NodeExistsException ex) {
                        System.out.println("Boss exists");
                    }
                }
                 */
                children.forEach(child -> {
                    try {
                        byte[] data = zk.getData("/" + child, false, null);
                        if (data == null) {
                            data = ("no data for /" + child).getBytes();
                        }
                        System.out.println("    " + child + ": " + new String(data));
                    } catch (KeeperException | InterruptedException ex) {
                        System.out.println("    " + child + ": " + ex.getMessage());
                    }
                });
            } catch (KeeperException|InterruptedException ex) {
                System.out.println("Got exception trying to get children: " + ex.getMessage());
            }

        }
    };

    @Override
    public void run() {
        System.out.println("Using connectString " + connectString);
        try {
            zk = new ZooKeeper(connectString, 10000, e -> {
               if (isFatal(e.getState())) {
                   System.out.println("ZooKeeper session failed: " + e);
                   System.exit(2);
               }
            });
            name = zk.create("/simple-", description.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("I am " + name);

            System.out.println("Trying to become the boss!");
            tryToBeBoss();
            childrenWatcher.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, null, "/"));
            for (int i = 0; i < 10; i++) {
                zk.setData(name, (description + " is bored... " + i).getBytes(), -1);
                Thread.sleep(10000);
            }
        } catch (IOException | InterruptedException | KeeperException e) {
            System.out.println("ZooKeeper session create failed: " + e);
            System.exit(2);
        }
    }

    private void tryToBeBoss() throws KeeperException, InterruptedException {
        try {
            zk.create("/boss", name.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException ee) {
            try {
                var data = zk.getData("/boss", event -> {
                    System.out.println("got event: " + event);
                        try {
                            tryToBeBoss();
                        } catch (KeeperException | InterruptedException e) {
                            System.out.println("Got exception when becoming boss: " + e);
                        }
                }, null);
            } catch (KeeperException.NoNodeException | KeeperException.ConnectionLossException e) {
                tryToBeBoss();
            }
            System.out.println("There is already a boss: " + new String(data));
        }
    }

    private static boolean isFatal(Watcher.Event.KeeperState state) {
        return switch (state) {
            case Closed, Expired, AuthFailed -> true;
            default -> false;
        };
    }
}
