package cn.junety.bloomfilter;

/**
 * Created by caijt on 2017/2/9.
 *
 * 假设现在有n个bit位，m个数据，x个哈希函数
 * 对于一次哈希计算，命中某一bit位的概率为：1/n, 不命中则为: 1-1/n
 * 对一个数据进行x次哈希，不命中某一bit位的概率为：(1-1/n)^x
 * 对于m个数据，不命中某一bit位的概率为：(1-1/n)^(xm)
 * 所以在m个数据的布隆过滤器中, 某一bit位为1的概率为：1-(1-1/n)^(xm)
 * 所以，对于一次查询，误判的概率为：(1-(1-1/n)^(xm))^x
 */
public class FalseHitRateCalc {

    private static final long MB = 1024 * 1024 * 8L;

    private static double quickPow(double p, long times) {
        double ans = 1.0;
        while(times != 0) {
            if((times & 1) == 1) ans *= p;
            times >>= 1;
            p = p * p;
        }
        return ans;
    }

    private static void calc(long bloomSize, long initDataSize, long hashFuncCount) {
        System.out.printf("当布隆过滤器的大小为%d, 数据量为%d, 哈希函数个数为%d时:\n",
                bloomSize, initDataSize, hashFuncCount);
        double p0 = 1.0 - 1.0/bloomSize;
        double p1 = quickPow(p0, initDataSize * hashFuncCount);
        //System.out.printf("布隆过滤器某一位为0的概率为: %f\n", p1);
        double p2 = 1 - p1;
        //System.out.printf("布隆过滤器某一位为1的概率为: %f\n", p2);
        double p3 = quickPow(p2, hashFuncCount);
        System.out.printf("误判的概率为: %.3f%%\n", p3 * 100);
        System.out.printf("正确性为: %.3f%%\n", (1.0 - p3) * 100.0);
        System.out.printf("使用的内存约为: %.3fMB\n", 1.0*bloomSize/MB);
        System.out.printf("=========================================================================\n\n");
    }

    public static void main(String[] args) {
        for(long i = 1; i <= 10; i++) {
            calc(2 << 27,25000000L, i);
        }

    }
}