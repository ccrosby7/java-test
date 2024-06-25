package com.real.matcher.thread;

import com.real.matcher.constants.Constants;
import com.real.matcher.model.Movie;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

public class PopulateMovies implements Callable<Void> {
    List<CSVRecord> csvRecords;

    ConcurrentMap<String, List<Integer>> masterMovieIdMap;
    ConcurrentMap<Integer, Movie> masterMovieDataMap;

    public PopulateMovies(List<CSVRecord> records, ConcurrentMap<String, List<Integer>> masterMovieIdMap,
                          ConcurrentMap<Integer, Movie> masterMovieDataMap) {
        this.csvRecords = records;
        this.masterMovieIdMap = masterMovieIdMap;
        this.masterMovieDataMap = masterMovieDataMap;
    }

    @Override
    public Void call() {
        HashMap<String, List<Integer>> resultIdMap = new HashMap<>(csvRecords.size());
        HashMap<Integer, Movie> resultMovieMap = new HashMap<>(csvRecords.size());


        if(csvRecords.isEmpty()){
            return null;
        }

        for (CSVRecord csvRecord : csvRecords) {
            Movie movie = new Movie();
            int id = Integer.parseInt(csvRecord.get(Constants.ID));
            String title = csvRecord.get(Constants.TITLE).toLowerCase();
            String year = csvRecord.get(Constants.YEAR);

            movie.setId(id);
            movie.setYear(year);
            resultMovieMap.put(id, movie);

            if(resultIdMap.containsKey(title)) {
                var newIdListWithTitle = resultIdMap.get(title);

                resultIdMap.replace(title, newIdListWithTitle);
            } else {
                resultIdMap.put(title, Collections.singletonList(id));
            }

        }

        try {
            resultIdMap.forEach((title, movieIds) -> masterMovieIdMap.computeIfAbsent(title, masterIdMap -> new ArrayList<>()).addAll(movieIds));
        } catch (UnsupportedOperationException e){
            e.printStackTrace();
        }

        masterMovieDataMap.putAll(resultMovieMap); //No collisions


        return null;
    }
}
