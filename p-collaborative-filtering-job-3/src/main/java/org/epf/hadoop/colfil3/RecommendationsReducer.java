package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RecommendationsReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Liste pour stocker les suggestions
        List<String> recommendations = new ArrayList<>();

        for (Text val : values) {
            recommendations.add(val.toString()); // user:commonRelations
        }

        // Trier par le nombre de relations communes (valeur après ":")
        Collections.sort(recommendations, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int count1 = Integer.parseInt(o1.split(":")[1]);
                int count2 = Integer.parseInt(o2.split(":")[1]);
                return Integer.compare(count2, count1); // Tri décroissant
            }
        });

        // Garder seulement les 5 premières suggestions
        List<String> topRecommendations = recommendations.subList(0, Math.min(5, recommendations.size()));

        // Construire une sortie lisible
        StringBuilder sb = new StringBuilder();
        for (String recommendation : topRecommendations) {
            sb.append(recommendation).append(", ");
        }

        // Enlever la dernière virgule et espace
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 2);
        }

        result.set(sb.toString());
        context.write(key, result);
    }
}