import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DnaCount {

    // --- MAPPER ---
    public static class DnaMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text ngram = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Παίρνουμε τη γραμμή και αφαιρούμε τυχόν κενά (whitespace)
            String line = value.toString().trim();
            int len = line.length();

            // Η εκφώνηση ζητάει 2-άδες, 3-άδες και 4-άδες
            // Οπότε κάνουμε loop για n = 2, 3, 4
            for (int n = 2; n <= 4; n++) {
                // Διατρέχουμε τη γραμμή.
                // Το όριο είναι: len - n + 1, ώστε να μην βγούμε εκτός ορίων (IndexOutOfBounds)
                for (int i = 0; i < len - n + 1; i++) {
                    // Εξαγωγή του substring (π.χ. "AG", "AGC", "AGCT")
                    String sub = line.substring(i, i + n);

                    // Έλεγχος (προαιρετικός): Βεβαιωνόμαστε ότι περιέχει μόνο A,G,C,T
                    // Αν το αρχείο είναι καθαρό, αυτό δεν χρειάζεται, αλλά είναι καλή πρακτική.
                    if (sub.matches("[AGCT]+")) {
                        ngram.set(sub);
                        context.write(ngram, one);
                    }
                }
            }
        }
    }

    // --- REDUCER ---
    // Ο Reducer είναι ακριβώς ο ίδιος με το WordCount (απλή άθροιση)
    public static class DnaReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // --- MAIN ---
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length != 2) {
            System.err.println("Usage: DnaCount <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "DNA N-gram Count");
        job.setJarByClass(DnaCount.class);

        job.setMapperClass(DnaMapper.class);
        job.setCombinerClass(DnaReducer.class); // Βοηθάει στην ταχύτητα
        job.setReducerClass(DnaReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}