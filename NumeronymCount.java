
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NumeronymCount {

    // --- MAPPER CLASS ---
    public static class NumeronymMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Μετατροπή γραμμής σε tokens
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String rawToken = itr.nextToken();

                // Καθαρισμός: Κρατάμε μόνο γράμματα και μετατρέπουμε σε πεζά
                String cleanToken = rawToken.replaceAll("[^a-zA-Z]", "").toLowerCase();

                // Έλεγχος μήκους: Αγνοούμε λέξεις μικρότερες από 3 χαρακτήρες [cite: 13, 15]
                if (cleanToken.length() >= 3) {
                    // Δημιουργία Numeronym: Πρώτο γράμμα + (μήκος - 2) + Τελευταίο γράμμα [cite: 12]
                    char first = cleanToken.charAt(0);
                    char last = cleanToken.charAt(cleanToken.length() - 1);
                    int middleCount = cleanToken.length() - 2;

                    String numeronym = first +String.valueOf(middleCount) + last;

                    word.set(numeronym);
                    context.write(word, one);
                }
            }
        }
    }

    // --- REDUCER CLASS ---
    public static class NumeronymReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private int minCount = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Ανάκτηση της παραμέτρου k από το configuration [cite: 17]
            Configuration conf = context.getConfiguration();
            minCount = conf.getInt("min.count", 1); // Default 1 αν δεν δοθεί
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Εμφάνιση αποτελέσματος μόνο αν το πλήθος είναι >= k [cite: 18]
            if (sum >= minCount) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    // --- MAIN / DRIVER ---
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Έλεγχος ορισμάτων: Input, Output, K
        if (args.length != 3) {
            System.err.println("Usage: NumeronymCount <input path> <output path> <k>");
            System.exit(-1);
        }

        // Ορισμός της παραμέτρου k στο configuration
        conf.setInt("min.count", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "numeronym count");
        job.setJarByClass(NumeronymCount.class);

        job.setMapperClass(NumeronymMapper.class);
        job.setCombinerClass(NumeronymReducer.class); // Προσοχή: Combiner μπορεί να χρησιμοποιηθεί, αλλά το φίλτρο >=k πρέπει να γίνει σίγουρα στον Reducer.
        job.setReducerClass(NumeronymReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}