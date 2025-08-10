package cn.think.in.java.raft.server.impl;

import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
/**
 * 测试RocksDB数据一致性
 */
@Slf4j
@RunWith(Parameterized.class)
public class DefaultStateMachineDataTest {

    // 初始化RocksDB库
    static {
        RocksDB.loadLibrary();
    }

    // 节点端口列表，根据实际情况修改
    @Parameterized.Parameters(name = "节点端口: {0}")
    public static Collection<Integer> nodePorts() {
        return Arrays.asList(8775, 8776, 8777, 8778, 8779);
    }

    // 当前测试的节点端口
    @Parameterized.Parameter
    public int nodePort;

    // 所有节点的数据集合，用于对比一致性
    private static Map<Integer, Map<String, String>> allNodeData = new HashMap<>();

    // RocksDB数据根目录
    private static final String BASE_DB_DIR = "../rocksDB-raft/";

    /**
     * 读取当前节点的所有数据
     */
    @Test
    public void testNodeData() throws RocksDBException {
        String stateMachineDir = BASE_DB_DIR + nodePort + "/stateMachine";
        File dbDir = new File(stateMachineDir);

        // 验证数据库目录存在
        org.junit.Assert.assertTrue(
                "节点[" + nodePort + "]的RocksDB目录不存在: " + stateMachineDir,
                dbDir.exists() && dbDir.isDirectory()
        );

        // 打开数据库并读取数据
        RocksDB rocksDB = null;
        RocksIterator iterator = null;
        try {
            Options options = new Options();
            options.setCreateIfMissing(false);
            //以只读模式打开数据库
            rocksDB = RocksDB.openReadOnly(options, stateMachineDir);

            iterator = rocksDB.newIterator();
            Map<String, String> nodeData = new HashMap<>();

            // 遍历所有键值对
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key());
                String value = new String(iterator.value());
                nodeData.put(key, value);
                log.info("节点[{}] - 键: {}, 值: {}", nodePort, key, value);
            }

            // 存储当前节点的数据供后续对比
            allNodeData.put(nodePort, nodeData);
            log.info("节点[{}]共读取到{}条数据", nodePort, nodeData.size());

        } finally {
            if (iterator != null) {
                iterator.close();
            }
            if (rocksDB != null) {
                rocksDB.close();
            }
        }
    }

    /**
     * 所有节点测试完成后，验证数据一致性
     */
    @AfterClass
    public static void verifyDataConsistency() {
        log.info("开始验证所有节点的数据一致性...");

        if (allNodeData.isEmpty()) {
            log.warn("没有读取到任何节点的数据，无法验证一致性");
            return;
        }

        // 以第一个节点的数据为基准
        Map.Entry<Integer, Map<String, String>> firstEntry = allNodeData.entrySet().iterator().next();
        int basePort = firstEntry.getKey();
        Map<String, String> baseData = firstEntry.getValue();

        // 与其他节点的数据进行对比
        for (Map.Entry<Integer, Map<String, String>> entry : allNodeData.entrySet()) {
            int nodePort = entry.getKey();
            if (nodePort == basePort) {
                continue;
            }

            Map<String, String> nodeData = entry.getValue();

            // 验证数据量是否一致
            org.junit.Assert.assertEquals(
                    "节点[" + nodePort + "]与基准节点[" + basePort + "]的数据量不一致",
                    baseData.size(),
                    nodeData.size()
            );

            // 验证每个键值对是否一致
            for (Map.Entry<String, String> dataEntry : baseData.entrySet()) {
                String key = dataEntry.getKey();
                String baseValue = dataEntry.getValue();
                String nodeValue = nodeData.get(key);

                org.junit.Assert.assertNotNull(
                        "节点[" + nodePort + "]缺少键: " + key,
                        nodeValue
                );

                org.junit.Assert.assertEquals(
                        "节点[" + nodePort + "]与基准节点[" + basePort + "]的键[" + key + "]值不一致",
                        baseValue,
                        nodeValue
                );
            }
        }

        log.info("所有节点的数据一致性验证通过！");
    }
}
