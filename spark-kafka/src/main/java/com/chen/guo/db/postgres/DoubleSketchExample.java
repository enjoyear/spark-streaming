package com.chen.guo.db.postgres;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;


public class DoubleSketchExample {
    public static void main(String[] args) throws IOException {
        System.out.println("Example 1: Reference https://datasketches.apache.org/docs/KLL/KLLSketch.html");
        example();

        System.out.println("Example 2: Reference https://datasketches.apache.org/docs/Quantiles/QuantilesJavaExample.html");
        SerializeSketches();
        DeserializeSketches();
    }

    private static void example() {
        UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().build();

        int n = 1000000;
        for (int i = 0; i < n; i++) {
            sketch.update(i);
        }

        System.out.printf("median is %f%n", sketch.getQuantile(0.5));  // 498971.0
        System.out.printf("rankOf5000 is %f%n", sketch.getRank(5000));  // 498971.0
    }

    private static void SerializeSketches() throws IOException {
        Random rand = new Random();

        // Sketch 1
        File file1 = new File("/tmp/datasketches/QuantilesDoublesSketch1.bin");
        file1.getParentFile().mkdirs();
        if (file1.exists()) {
            file1.delete();
        }
        file1.createNewFile();
        UpdateDoublesSketch sketch1 = DoublesSketch.builder().build(); // default k=128
        for (int i = 0; i < 10000; i++) {
            sketch1.update(rand.nextGaussian()); // mean=0, stddev=1
        }
        try (FileOutputStream outputStream = new FileOutputStream(file1)) {
            outputStream.write(sketch1.toByteArray());
        }


        // Sketch 2
        File file2 = new File("/tmp/datasketches/QuantilesDoublesSketch2.bin");
        file2.getParentFile().mkdirs();
        if (file2.exists()) {
            file2.delete();
        }
        file2.createNewFile();
        UpdateDoublesSketch sketch2 = DoublesSketch.builder().build(); // default k=128
        for (int i = 0; i < 10000; i++) {
            sketch2.update(rand.nextGaussian() + 1); // shift the mean for the second sketch
        }
        try (FileOutputStream outputStream = new FileOutputStream(file2)) {
            outputStream.write(sketch2.toByteArray());
        }
    }

    private static void DeserializeSketches() throws IOException {
        FileInputStream in1 = new FileInputStream("/tmp/datasketches/QuantilesDoublesSketch1.bin");
        byte[] bytes1 = new byte[in1.available()];
        in1.read(bytes1);
        in1.close();
        DoublesSketch sketch1 = DoublesSketch.wrap(Memory.wrap(bytes1));

        FileInputStream in2 = new FileInputStream("/tmp/datasketches/QuantilesDoublesSketch2.bin");
        byte[] bytes2 = new byte[in2.available()];
        in2.read(bytes2);
        in2.close();
        DoublesSketch sketch2 = DoublesSketch.wrap(Memory.wrap(bytes2));

        DoublesUnion union = DoublesUnion.builder().build(); // default k=128
        union.update(sketch1);
        union.update(sketch2);
        DoublesSketch result = union.getResult();
        // Debug output from the sketch
        System.out.println(result.toString());

        System.out.println("Min, Median, Max values");
        System.out.println(Arrays.toString(result.getQuantiles(new double[]{0, 0.5, 1})));

        System.out.println("Probability Histogram: estimated probability mass in 4 bins: (-inf, -2), [-2, 0), [0, 2), [2, +inf)");
        System.out.println(Arrays.toString(result.getPMF(new double[]{-2, 0, 2})));

        System.out.println("Frequency Histogram: estimated number of original values in the same bins");
        double[] histogram = result.getPMF(new double[]{-2, 0, 2});
        for (int i = 0; i < histogram.length; i++) {
            histogram[i] *= result.getN(); // scale the fractions by the total count of values
        }
        System.out.println(Arrays.toString(histogram));
    }
}
