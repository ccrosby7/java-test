package com.real.matcher.thread;

import com.real.matcher.Matcher;
import com.real.matcher.constants.Constants;
import com.real.matcher.model.Movie;
import org.apache.commons.csv.CSVRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

public class MatchXbox implements Callable<Set<Matcher.IdMapping>> {
    List<CSVRecord> csvRecords;
    ConcurrentMap<String, List<Integer>> masterMovieIdMap;
    ConcurrentMap<Integer, Movie> masterMovieDataMap;
    private final double CONFIDENCE_THRESHOLD = .50d;

    public MatchXbox(List<CSVRecord> records, ConcurrentMap<String, List<Integer>> masterMovieIdMap,
                     ConcurrentMap<Integer, Movie> masterMovieDataMap) {
        this.csvRecords = records;
        this.masterMovieIdMap = masterMovieIdMap;
        this.masterMovieDataMap = masterMovieDataMap;
    }

    @Override
    public Set<Matcher.IdMapping> call() {
        Set<Matcher.IdMapping> result = new HashSet<>();

        if(csvRecords.isEmpty()){
            return Collections.emptySet();
        }

        for (CSVRecord csvRecord : csvRecords) {

            if(!csvRecord.get(Constants.XBOX_MEDIA_TYPE).equals(Constants.XBOX_MOVIE)) {
                continue;
            }

            String title = csvRecord.get(Constants.XBOX_TITLE).toLowerCase().trim();

            if(masterMovieIdMap.containsKey(title)) {
                List<Integer> titleMatches = masterMovieIdMap.get(title);

                int highestConfidenceMatchId = 0;
                double highestConfidenceMatchScore = 0;

                for(int id : titleMatches) {
                    double confidence;
                    Movie movieMatch = masterMovieDataMap.get(id);

                    List<String> dbActors = movieMatch.getActors();
                    List<String> dbDirectors = movieMatch.getDirectors();

                    String xboxDirector = csvRecord.get(Constants.XBOX_DIRECTOR);
                    String xboxActorString = csvRecord.get(Constants.XBOX_ACTOR);
                    List<String> xboxActors = Arrays.asList(xboxActorString.split(", "));

                    int totalCast = dbActors.size() + dbDirectors.size();

                    dbActors.retainAll(xboxActors);
                    double matchedCast = dbActors.size();
                    if(dbDirectors.contains(xboxDirector)) { matchedCast++; }

                    confidence = 100 * (matchedCast/totalCast);

                    //Of all entries with the same title, which is closest in cast?
                    if(confidence > highestConfidenceMatchScore) {
                        highestConfidenceMatchId = id;
                        highestConfidenceMatchScore = confidence;
                    }

                }

                if(highestConfidenceMatchScore >= CONFIDENCE_THRESHOLD) {
                    String xboxId = csvRecord.get(Constants.XBOX_MEDIA_ID);
                    result.add(new Matcher.IdMapping(highestConfidenceMatchId, xboxId));
                }
            }

        }

        return result;
    }
}
