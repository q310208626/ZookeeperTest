package com.lsj.test.zkcli;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class MyWatcher implements Watcher {

    Logger logger = Logger.getLogger(Watcher.class);

    public void process(WatchedEvent watchedEvent) {
        // None表示keeper连接变化
        if(Event.EventType.None ==watchedEvent.getType()){
            if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                logger.debug("zk 连接已经建立");
                ZkConnectCountDownUtils.afterConnect();
            }else{
                logger.debug("zk 连接未建立");
            }
        }else{
            if(Event.EventType.NodeCreated == watchedEvent.getType()){
                logger.debug("=======path create:"+watchedEvent.getPath());
            }else if(Event.EventType.NodeDeleted == watchedEvent.getType()){
                logger.debug("=======path delete:"+watchedEvent.getPath());
            }else if(Event.EventType.NodeDataChanged == watchedEvent.getType()){
                logger.debug("=======path dataChange:"+watchedEvent.getPath());
            }else if(Event.EventType.NodeChildrenChanged == watchedEvent.getType()){
                logger.debug("=======path nodeChildChange:"+watchedEvent.getPath());
            }
        }

    }
}
