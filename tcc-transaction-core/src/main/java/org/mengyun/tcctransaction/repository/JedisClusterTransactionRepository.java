package org.mengyun.tcctransaction.repository;

import org.mengyun.tcctransaction.repository.helper.JedisClusterCommands;
import org.mengyun.tcctransaction.repository.helper.RedisCommands;
import org.mengyun.tcctransaction.repository.helper.ShardHolder;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
// redis cluster 实现
public class JedisClusterTransactionRepository extends AbstractRedisTransactionRepository {
    // redis cluster
    private JedisCluster jedisCluster;
    // 包装成 JedisClusterCommands 返回
    @Override
    protected RedisCommands getRedisCommands(byte[] shardKey) {
        return new JedisClusterCommands(jedisCluster);
    }

    @Override
    protected ShardHolder<Jedis> getShardHolder() {
        return new JedisClusterShardHolder();
    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    public void setJedisCluster(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    class JedisClusterShardHolder implements ShardHolder<Jedis> {

        private static final int MASTER_NODE_INDEX = 2;

        private List<Jedis> allShards = new ArrayList<>();

        @Override
        public void close() throws IOException {

            for (Jedis jedis : allShards) {
                try {
                    if (jedis != null) {
                        jedis.close();
                    }
                } catch (Exception e) {
                    log.error("close jedis error", e);
                }
            }
        }

        @Override
        public List<Jedis> getAllShards() {

            if (allShards.isEmpty()) {
                // 获得所有节点
                Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
                // 获得所有 master 节点的 ip+port
                Set<String> masterNodeKeys = getMasterNodeKeys(clusterNodes);

                for (String masterNodeKey : masterNodeKeys) { // 遍历每个 master node
                    // 得到 jedis pool
                    JedisPool jedisPool = clusterNodes.get(masterNodeKey);

                    if (jedisPool != null) {
                        Jedis jedis = clusterNodes.get(masterNodeKey).getResource(); // 得到一个连接
                        allShards.add(jedis);
                    }
                }
                // 排序
                allShards.sort(new AbstractRedisTransactionRepository.JedisComparator());
            }

            return allShards;
        }

        private Set<String> getMasterNodeKeys(Map<String, JedisPool> clusterNodes) {
            Set<String> masterNodeKeys = new HashSet<>();
            // 遍历
            for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
                // 得到一个 jedis 连接
                try (Jedis jedis = entry.getValue().getResource()) {
                    // 所有槽位
                    List<Object> slots = jedis.clusterSlots();

                    for (Object slotInfoObj : slots) { // 遍历每个槽位
                        List<Object> slotInfo = (List<Object>) slotInfoObj; // 得到槽位的描述信息 slotInfo
                        // master node 是 slotInfo 的第 3 个元素
                        if (slotInfo.size() <= MASTER_NODE_INDEX) {
                            continue;
                        }

                        // 得到  hostInfos
                        List<Object> hostInfos = (List<Object>) slotInfo.get(MASTER_NODE_INDEX);
                        if (hostInfos.isEmpty()) {
                            continue;
                        }

                        // at this time, we just use master, discard slave information
                        HostAndPort node = generateHostAndPort(hostInfos); // 得到 ip + port
                        masterNodeKeys.add(JedisClusterInfoCache.getNodeKey(node)); // 加入到集合
                    }

                    break;
                } catch (Exception e) {
                    // try next jedispool
                }
            }
            return masterNodeKeys;
        }

        private HostAndPort generateHostAndPort(List<Object> hostInfos) {
            String host = encode((byte[]) hostInfos.get(0));
            int port = ((Long) hostInfos.get(1)).intValue();
            return new HostAndPort(host, port);
        }


        private String encode(final byte[] data) {
            try {
                return new String(data, Protocol.CHARSET);
            } catch (UnsupportedEncodingException e) {
                throw new JedisException(e);
            }
        }
    }

}

