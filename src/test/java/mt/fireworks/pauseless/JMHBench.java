package mt.fireworks.pauseless;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.*;

import lombok.Cleanup;


/**
 * CPU: i7-11850H @ 2.5 GHz
 * VM version: JDK 17.0.9, OpenJDK 64-Bit Server VM, 17.0.9+9
 *
 * Benchmark               Mode  Cnt   Score   Error   Units
 * JMHBench.internTrie    thrpt    5  30.659 ± 1.262  ops/us
 * JMHBench.newString     thrpt    5  79.764 ± 4.203  ops/us
 * JMHBench.stringIntern  thrpt    5  16.173 ± 2.413  ops/us
 */

public class JMHBench {
    static String filename = "pauseless.txt";

    @State(Scope.Benchmark)
    public static class BenchState2 {
        String path;
        byte[] data;
        int dataIdx = 0;
        InternTrie<String> trie;

        @Setup
        public void setup() throws IOException {
            trie = new InternTrie<>();
            path = System.getProperty("java.io.tmpdir") + JMHBench.filename;
            File f = new File(path);
            @Cleanup FileInputStream fis = new FileInputStream(f);
            byte[] d = new byte[(int) f.length()];
            fis.read(d);
            this.data = d;
            dataIdx = 0;
        }
    }

    public static void main(String[] args) throws Exception {
        String path = setupBechmarkData(10_000, 100, 1, 15);
        long len = new File(path).length();
        System.out.println("Testa data prepared at: '" + path + "' (" + len + " bytes)");

        Options opt = new OptionsBuilder()
                .include("mt\\.fireworks\\.pauseless\\.JMHBench\\..*")
                .forks(1)
                .warmupIterations(1)
                .warmupTime(TimeValue.seconds(3))
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(3))
                .timeUnit(TimeUnit.MICROSECONDS)
                .mode(Mode.Throughput)
                .shouldDoGC(false)
                .build();

        new Runner(opt).run();
    }



    @Benchmark
    public long newString(BenchState2 state) throws Exception {
        byte[] data = state.data;
        int off = state.dataIdx;
        int len = len(data, off);
        if (len < 0) {
            state.dataIdx = 0;
            len = len(data, 0);
        }

        String str = new String(data, off, len, US_ASCII);
        return str.length();
    }


    @Benchmark
    public long stringIntern(BenchState2 state) throws Exception {
        byte[] data = state.data;
        int off = state.dataIdx;
        int len = len(data, off);
        if (len < 0) {
            state.dataIdx = 0;
            len = len(data, 0);
        }

        String str = new String(data, off, len, US_ASCII).intern();
        return str.length();
    }


    @Benchmark
    public long internTrie(BenchState2 state) throws Exception {
        byte[] data = state.data;
        int off = state.dataIdx;
        int len = len(data, off);
        if (len < 0) {
            state.dataIdx = 0;
            len = len(data, 0);
        }

        String str = state.trie.intern(data, off, len , (objData, o, l) -> new String(objData, o, l, US_ASCII));
        return str.length();
    }



    static int len(byte[] data, int off) {
        if (off >= data.length - 1) return - 1;

        for (int len = 0; len < data.length; len++) {
            byte b = data[off + len];
            if (b == (char) '\n') return len;
        }

        return -1;
    }


    public static String setupBechmarkData(
            int numberOfUniqueStrings, /* how many strings are generated */
            int stringReuse,           /* how many times are string reused on average */
            int minStringLen,
            int maxStringLen
    ) throws IOException {
        String tmpdir = System.getProperty("java.io.tmpdir");
        File testData = new File(tmpdir + JMHBench.filename);

        ThreadLocalRandom rng = ThreadLocalRandom.current();

        String[] randomStrings = new String[numberOfUniqueStrings];
        for (int i = 0; i < randomStrings.length; i++) {
            int len = rng.nextInt(minStringLen, maxStringLen);
            randomStrings[i] = RandomStringUtils.randomAlphanumeric(len);
        }

        FileWriter fw = new FileWriter(testData, false);
        @Cleanup BufferedWriter bw = new BufferedWriter(fw);
        for (int i = 0; i < numberOfUniqueStrings * stringReuse; i++) {
            int strIdx = rng.nextInt(randomStrings.length);
            String str = randomStrings[strIdx];
            bw.write(str);
            bw.write('\n');
        }

        String path = testData.getPath();
        return path;
    }

}
