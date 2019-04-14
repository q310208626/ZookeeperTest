package com.lsj.test;

import com.lsj.test.zkcli.MyWatcher;
import com.lsj.test.zkcli.ZkConnectCountDownUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ZkCliTest {

    Logger logger = Logger.getLogger(ZkCliTest.class);
    @Test
    public void test(){
        Watcher watcher = new MyWatcher();
        try {
            ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181",10000,watcher);
            ZkConnectCountDownUtils.beforeConnect();

            // 判断节点是否存在，存在则删除
            if(existZNode(zooKeeper,"/my_zk_node",watcher)){
                zooKeeper.addAuthInfo("digest","zk_user:zk_password".getBytes());
                rmrZNode(zooKeeper,"/my_zk_node",-1);
            }

            // 创建节点，获取节点信息
            createZNode(zooKeeper,"/my_zk_node","my_zk_data", ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.debug("=====create ZnodeData:"+getZNodeData(zooKeeper,"/my_zk_node",watcher));
            logger.debug("=====create ZnodeStat:"+getZNodeStat(zooKeeper,"/my_zk_node",watcher));

            // 更新节点信息
            setZNodeData(zooKeeper,"/my_zk_node","new_data",-1);
            logger.debug("=====new ZnodeData:"+getZNodeData(zooKeeper,"/my_zk_node",watcher));
            logger.debug("=====new ZnodeStat:"+getZNodeStat(zooKeeper,"/my_zk_node",watcher));

            // 设置节点ACL,获取ACL信息
            ACL acl = new ACL();
            Id id = new Id();
            //      设置权限模式为digest
            id.setScheme("digest");
            //      设置Id信息
            id.setId(DigestAuthenticationProvider.generateDigest("zk_user:zk_password"));
            //      设置permission
            acl.setPerms(ZooDefs.Perms.ALL);
            acl.setId(id);
            setZNodeACL(zooKeeper,"/my_zk_node", Collections.singletonList(acl),-1);
            List<ACL> acls = getZNodeAcl(zooKeeper,"/my_zk_node");
            acls.forEach(x->{
                logger.debug("=====ACL:"+x);
            });

            // 设置ACL后获取ZNode信息
            try{
                logger.debug("=====acl ZnodeData:"+getZNodeData(zooKeeper,"/my_zk_node",watcher));
            }
            catch (KeeperException e){
                logger.error(e);
            }

            // 添加鉴权信息后获取ZNode信息
            zooKeeper.addAuthInfo("digest","zk_user:zk_password".getBytes());
            logger.debug("=====auth ZnodeData:"+getZNodeData(zooKeeper,"/my_zk_node",watcher));


            // 创建子节点，获取子节点信息
            createZNode(zooKeeper,"/my_zk_node/child1","child1_data", ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            List<String> chlidPath = zooKeeper.getChildren("/my_zk_node",watcher);
            chlidPath.stream().forEach(x->logger.debug("=====pathChildren:"+x));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private boolean existZNode(ZooKeeper zooKeeper,String path,Watcher watcher) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/my_zk_node",watcher);
        if(null == stat) return false;
        return true;
    }

    private void deleteZNode(ZooKeeper zooKeeper,String path,int version) throws KeeperException, InterruptedException {
        zooKeeper.delete(path,version);
    }

    private void rmrZNode(ZooKeeper zooKeeper,String path,int version) throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(path,null);

        if(null != children || children.size() >0){
            for (String child : children) {
                rmrZNode(zooKeeper,path+"/"+child,version);
            }
        }
        deleteZNode(zooKeeper,path,version);
    }

    private void createZNode(ZooKeeper zooKeeper,String path,String data,ArrayList<ACL> acls,CreateMode createMode) throws KeeperException, InterruptedException {
        byte[] dataByte = null;
        if(null != data){
            dataByte = data.getBytes();
        }
        zooKeeper.create(path,dataByte, acls, createMode);
    }

    private String getZNodeData(ZooKeeper zooKeeper,String path,Watcher watcher) throws KeeperException, InterruptedException {
        String dataString = "";
        byte[] dataByte = zooKeeper.getData(path,watcher,null);
        if( null != dataByte){
            dataString = new String(dataByte);
        }
        return dataString;
    }

    private Stat getZNodeStat(ZooKeeper zooKeeper,String path,Watcher watcher) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        zooKeeper.getData(path,watcher,stat);
        return stat;
    }

    private void setZNodeData(ZooKeeper zooKeeper,String path,String data,int version) throws KeeperException, InterruptedException {
        byte[] newData = null;
        if(null != data){
            newData = data.getBytes();
        }
        zooKeeper.setData(path,newData,version);
    }

    private void setZNodeACL(ZooKeeper zooKeeper, String path, List<ACL> acls, int version) throws KeeperException, InterruptedException {
        zooKeeper.setACL(path,acls,version);
    }

    private List<ACL> getZNodeAcl(ZooKeeper zooKeeper,String path) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        List<ACL> acls = zooKeeper.getACL(path,stat);
        return acls;
    }
}
