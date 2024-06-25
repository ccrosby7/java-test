package com.real.matcher.thread;

import com.real.matcher.constants.Constants;
import com.real.matcher.model.Movie;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

public class PopulateRoles implements Callable<Void> {
    List<CSVRecord> csvRecords;
    ConcurrentMap<Integer, Movie> masterMovieDataMap;
    private static final Logger LOGGER = LoggerFactory.getLogger(PopulateRoles.class);

    public PopulateRoles(List<CSVRecord> records, ConcurrentMap<Integer, Movie> masterMovieDataMap) {
        this.csvRecords = records;
        this.masterMovieDataMap = masterMovieDataMap;
    }

    @Override
    public Void call() {
        HashMap<Integer, Movie> result = new HashMap<>(csvRecords.size()); //minor gain.

        if(csvRecords.isEmpty()){
            return null;
        }

        for (CSVRecord csvRecord : csvRecords) {
            int movieId = Integer.parseInt(csvRecord.get(Constants.MOVIE_ID));
            String castMemberName = csvRecord.get(Constants.CAST_MEMBER_NAME);
            String role = csvRecord.get(Constants.ROLE);

            if(masterMovieDataMap.containsKey(movieId)) {
                Movie masterMovie = masterMovieDataMap.get(movieId);

                Movie movie = new Movie();

                if(result.containsKey(movieId)){
                    movie = result.get(movieId);
                } else {
                    movie.setTitle(masterMovie.getTitle());
                    movie.setId(movieId);
                }

                if(role.equals(Constants.DIRECTOR)) {
                    movie.appendToDirectors(castMemberName);
                }
                if(role.equals(Constants.ACTOR)) {
                    movie.appendToActors(castMemberName);
                }
                result.put(movieId, movie);
            }
        }

        result.forEach((id, resultMovie) -> {
            var entry = masterMovieDataMap.computeIfPresent(id, (masterId, masterMovie) -> masterMovie.merge(resultMovie));
            if(null == entry){
                LOGGER.warn("Failed to update map with id: {}, title: {}", id, resultMovie.getTitle());
            }
        });


        return null;
    }

}
