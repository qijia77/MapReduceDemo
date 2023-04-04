package com.jia.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AprioriMapperReducer {

    // Mapper class
    public static class AprioriMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Set<Set<String>> frequentItemsets = new HashSet<>();

        private int minSupport;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String[] itemsets = context.getConfiguration().getStrings("itemsets");
            for (String itemset : itemsets) {
                Set<String> items = new HashSet<>(Arrays.asList(itemset.split(",")));
                frequentItemsets.add(items);
            }

            minSupport = context.getConfiguration().getInt("minSupport", 0);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] transaction = value.toString().split(",");
            Set<String> items = new HashSet<>(Arrays.asList(transaction));

            for (Set<String> itemset : frequentItemsets) {
                if (items.containsAll(itemset)) {
                    context.write(new Text(itemset.toString()), new IntWritable(1));
                }
            }
        }
    }

    // Reducer class
    public static class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int minSupport;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minSupport = context.getConfiguration().getInt("minSupport", 0);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Compute the count of the frequent itemset
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                count += iterator.next().get();
            }

            if (count >= minSupport) {
                context.write(key, new IntWritable(count));
            }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: AprioriMapperReducer <input> <output> <minSupport> [<numIterations>]");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        int minSupport = Integer.parseInt(args[2]);
        int numIterations = args.length > 3 ? Integer.parseInt(args[3]) : 1;
    }
}
