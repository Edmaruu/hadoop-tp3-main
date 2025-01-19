package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommendationsMapper extends Mapper<Object, Text, Text, Text> {
    private Text userKey = new Text();
    private Text userValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Exemple d'entrée : "abigailallan.abigailmorgan\t1"
        String[] line = value.toString().split("\t");
        if (line.length == 2) {
            String[] users = line[0].split("\\.");
            if (users.length == 2) {
                String user1 = users[0];
                String user2 = users[1];
                String commonRelations = line[1];

                // Émettre pour chaque utilisateur comme clé
                userKey.set(user1);
                userValue.set(user2 + ":" + commonRelations);
                context.write(userKey, userValue);

                userKey.set(user2);
                userValue.set(user1 + ":" + commonRelations);
                context.write(userKey, userValue);
            }
        }
    }
}
