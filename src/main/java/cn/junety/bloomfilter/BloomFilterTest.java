package cn.junety.bloomfilter;

import cn.junety.bloomfilter.util.DataReader;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.*;

/**
 * Created by caijt on 2017/2/9.
 *
 * 基于Redis的BloomFilter命中率测试
 */
public class BloomFilterTest {

    private static final int DEFAULT_SIZE = 2 << 27; //布隆过滤器的大小
    private static final int DEFAULT_HASH_COUNT = 8;
    private static final int[] seeds = {31,53,97,131,193,389,769,1543,3079,6151};

    private static final int PIPELINE_LIMIT = 1000;

    private static final String BLOOM_KEY = "bloom-filter";
    private static Set<String> dataSet;

    private static Jedis jedis;

    public static void main(String[] args) throws IOException {
        jedis = new Jedis("127.0.0.1", 6379);

        List<String> initData = readData("/Users/caijt/Downloads/custom-packet/packet-init.zip");
        initBloomFilter(initData);
        initDataSet(initData);

        filter("/Users/caijt/Downloads/custom-packet/packet-1.zip");
        filter("/Users/caijt/Downloads/custom-packet/packet-2.zip");
        filter("/Users/caijt/Downloads/custom-packet/packet-3.zip");
        filter("/Users/caijt/Downloads/custom-packet/packet-4.zip");
        filter("/Users/caijt/Downloads/custom-packet/packet-5.zip");

        jedis.close();
    }

    /**
     * 布隆过滤器初始化
     */
    private static void initBloomFilter(List<String> initData) throws IOException {
        long start = System.currentTimeMillis();
        System.out.println("Bloom Filter大小为:" + DEFAULT_SIZE);
        System.out.print("开始导入, 当前过滤进度: 0%");
        Pipeline pipeline = jedis.pipelined();
        int times = (initData.size() + PIPELINE_LIMIT - 1) / PIPELINE_LIMIT;
        int part = times / 20;
        for (int i = 0; i < times; i++) {
            if((i+1) % part == 0) {
                System.out.print(", " + (i+1)/part*5 + "%");
            }
            int begin = i * PIPELINE_LIMIT;
            int end = Math.min((i + 1) * PIPELINE_LIMIT, initData.size());
            for (int j = begin; j < end; j++) {
                add(pipeline, initData.get(j));
            }
            pipeline.sync();
        }
        pipeline.close();
        System.out.printf("\n初始化完成...耗时: %dms\n", (System.currentTimeMillis() - start));
        System.out.println("====================================================================");
    }

    private static void initDataSet(List<String> initData) {
        dataSet = new HashSet<>(initData);
    }

    private static List<String> readData(String zipPath) throws IOException {
        long start = System.currentTimeMillis();
        System.out.println("读取数据...");
        List<String> initData = DataReader.readZip(zipPath);
        System.out.printf("读取完成, 数据量为: %d, 耗时: %dms\n", initData.size(), (System.currentTimeMillis() - start));
        return initData;
    }

    /**
     * 误判率测试
     */
    private static void filter(String testZipPath) throws IOException {
        List<String> testData = readData(testZipPath);
        System.out.printf("测试数据包: %s, 测试数据量:%d\n", testZipPath, testData.size());
        int include = 0;    //初始化数据包中存在的数据
        int hit = 0;        //误判的次数
        int part = testData.size() / 20;
        System.out.print("当前过滤进度: 0%");
        for (int i = 0; i < testData.size(); i++) {
            if((i+1) % part == 0) {
                System.out.print(", " + (i+1)/part*5 + "%");
            }
            if (dataSet.contains(testData.get(i))) {
                include++;
            } else {
                if (contains(testData.get(i))) {
                    hit++;
                }
            }
        }
        System.out.println("\nBloom Filter过滤结果:");
        System.out.printf("测试包的数据量为: %d, 其中有 %d 条数据与初始化包重复, 实际有效数据为:%d\n", testData.size(), include, (testData.size() - include));
        System.out.printf("误判的数据量为:%d, 误判率为:%.3f\n", hit, 1.0*hit/(testData.size()-include)*100);
        System.out.println("=====================================================================\n");
    }

    private static void add(String... values) throws IOException {
        Pipeline pipeline = jedis.pipelined();
        for(String value : values) {
            for (int i = 0; i < DEFAULT_HASH_COUNT; i++) {
                int pos = hash(value, i);
                pipeline.setbit(BLOOM_KEY, pos, true);
            }
        }
        pipeline.sync();
        pipeline.close();
    }

    private static void add(Pipeline pipeline, String... values) throws IOException {
        for(String value : values) {
            for (int i = 0; i < DEFAULT_HASH_COUNT; i++) {
                int pos = hash(value, i);
                pipeline.setbit(BLOOM_KEY, pos, true);
            }
        }
    }

    private static boolean contains(String value) throws IOException {
        Pipeline pipeline = jedis.pipelined();
        List<Response<Boolean>> list = new ArrayList<>();
        for(int i = 0; i < DEFAULT_HASH_COUNT; i++) {
            int pos = hash(value, i);
            list.add(pipeline.getbit(BLOOM_KEY, pos));
        }
        pipeline.sync();
        pipeline.close();
        boolean result = true;
        for (Response<Boolean> response : list) {
            result = result & response.get();
        }
        return result;
    }

    private static int hash(String value, int index) {
        return hash(value, DEFAULT_SIZE, seeds[index]);
    }

    private static int hash(String value, int cap, int seed) {
        int result = 0;
        int len = value.length();
        for (int i = 0; i < len; i++) {
            result = seed * result + value.charAt(i);
        }
        return (cap - 1) & result;
    }
}
