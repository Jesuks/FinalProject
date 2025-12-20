package aqua;

public class Snapshot {
    // 原始数据均使用 long (Int64) 存储，避免精度丢失
    public long tradeTime;
    public long tradingDay;
    public long[] bp = new long[5];
    public long[] bv = new long[5];
    public long[] ap = new long[5];
    public long[] av = new long[5];

    public long tBidVol;
    public long tAskVol;
    public long wBidPrc;
    public long wAskPrc;

    /**
     * 直接解析原始 CSV 行数据
     * 对应 Sample: 20240102,092500,...,tBidVol,tAskVol,wBidPrc,wAskPrc,openInterest,bp1,bv1,ap1,av1...
     */

    public Snapshot(String line) {
        // 默认值都是 0；如果某字段缺失就保持默认
        int field = 0;
        int start = 0;
        int len = line.length();

        for (int i = 0; i <= len; i++) {
            if (i == len || line.charAt(i) == ',') {
                // 当前字段范围是 [start, i)
                if (field == 0) {
                    this.tradingDay = parseLongFast(line, start, i);
                } else if (field == 1) {
                    this.tradeTime = parseLongFast(line, start, i);
                } else if (field == 12) {
                    this.tBidVol = parseLongFast(line, start, i);
                } else if (field == 13) {
                    this.tAskVol = parseLongFast(line, start, i);
                } else if (field == 14) {
                    this.wBidPrc = parseLongFast(line, start, i);
                } else if (field == 15) {
                    this.wAskPrc = parseLongFast(line, start, i);
                } else if (field >= 17 && field <= 36) {
                    long v = parseLongFast(line, start, i);
                    int off = field - 17;      // 0..19
                    int lvl = off >> 2;        // /4, 0..4
                    int pos = off & 3;         // %4, 0..3
                    switch (pos) {
                        case 0: bp[lvl] = v; break;
                        case 1: bv[lvl] = v; break;
                        case 2: ap[lvl] = v; break;
                        case 3: av[lvl] = v; break;
                    }
                }

                field++;
                if (field > 36) {
                    // 你只需要到 av5（字段 36），后面列直接不扫了
                    break;
                }
                start = i + 1;
            }
        }
    }

    /** 解析 s[start,end) 的 long；跳过前后空格；不产生 substring */
    private static long parseLongFast(String s, int start, int end) {
        // trim left
        while (start < end && s.charAt(start) == ' ') start++;
        // trim right
        while (end > start && s.charAt(end - 1) == ' ') end--;

        if (start >= end) return 0L;

        int i = start;
        long sign = 1L;
        char c = s.charAt(i);
        if (c == '-') { sign = -1L; i++; }

        long n = 0L;
        for (; i < end; i++) {
            c = s.charAt(i);
            if (c < '0' || c > '9') break; // 遇到非数字就停
            n = n * 10L + (c - '0');
        }
        return n * sign;
    }
}
