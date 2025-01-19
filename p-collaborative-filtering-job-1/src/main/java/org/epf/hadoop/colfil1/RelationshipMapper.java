package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {
    //private Text user = new Text();
    //private Text friend = new Text();

    @Override
    protected void map(LongWritable key, Relationship value, Context context) throws IOException, InterruptedException {
        String userA = value.getId1();
        String userB = value.getId2();

        // Vérification de données valides avant d'émettre
        if (!userA.isEmpty() && !userB.isEmpty()) {
            context.write(new Text(userA), new Text(userB));
            context.write(new Text(userB), new Text(userA));
        }
    }
}
