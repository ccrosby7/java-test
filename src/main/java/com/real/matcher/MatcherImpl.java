package com.real.matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.real.matcher.constants.Constants;
import com.real.matcher.model.Movie;
import com.real.matcher.thread.MatchXbox;
import com.real.matcher.thread.PopulateMovies;
import com.real.matcher.thread.PopulateRoles;
import com.real.matcher.util.Util;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.real.matcher.util.Util.getStreamData;

public class MatcherImpl implements Matcher {


  ConcurrentMap<String, List<Integer>> masterMovieIdMap = new ConcurrentHashMap<>();
  ConcurrentMap<Integer, Movie> masterMovieDataMap = new ConcurrentHashMap<>();

  private static final Logger LOGGER = LoggerFactory.getLogger(MatcherImpl.class);

  public MatcherImpl(CsvStream movieDb, CsvStream actorAndDirectorDb) {
    LOGGER.info("importing database");

    var movies = getStreamData(movieDb, "Movie DB");
    var actorsAndDirectors = getStreamData(actorAndDirectorDb, "Actors And Directors"); //make this file name

    internalDbToMap(movies.getRecords(), MovieOrRole.MOVIE);
    LOGGER.info("movie database imported");
    internalDbToMap(actorsAndDirectors.getRecords(), MovieOrRole.ROLE);
    LOGGER.info("role database imported");
    LOGGER.info("IdMap size: {}, dataMap size: {}", masterMovieIdMap.size(), masterMovieDataMap.size());

  }

  @Override
  public List<IdMapping> match(DatabaseType databaseType, CsvStream externalDb) {
    var data = getStreamData(externalDb, databaseType.name()); //make this file name

    switch(databaseType) {
      case XBOX:
        return matchXbox(data.getRecords(), databaseType);
      default:
        return Collections.emptyList();
    }
  }

  public List<IdMapping> matchXbox(List<CSVRecord> records, DatabaseType databaseType) {
    if(null == records || records.isEmpty()){
      LOGGER.error("No records to consume!");
      return Collections.emptyList();
    }

    Set<IdMapping> resultSet = new HashSet<>();

    List<List<CSVRecord>> recordChunks = Util.divideAndConquer(records);
    List<CompletableFuture<Set<IdMapping>>> results;

    switch(databaseType) {
      case XBOX:
        results = sendToProcessXbox(recordChunks);
        break;
      default:
        LOGGER.error("Database Type {} invalid!", databaseType.name());
        return Collections.emptyList();
    }
    
    var allFuturesResult = CompletableFuture.allOf(results.toArray(new CompletableFuture<?>[0]));

    try {
      allFuturesResult.get(); //waits for all to finish
    } catch (InterruptedException | ExecutionException e){
      LOGGER.error(Constants.FAILED_THREAD, e.getMessage());
      e.printStackTrace();
    }

    Set<IdMapping> threadResult;
    int totalSize = 0;
    for(CompletableFuture<Set<IdMapping>> res : results) {
      try {
        threadResult = res.get();
        totalSize = totalSize+threadResult.size();
        resultSet.addAll(threadResult);
      } catch (ExecutionException | InterruptedException e) {
        LOGGER.error(Constants.FAILED_THREAD, e.getMessage());
        e.printStackTrace();
      }
    }

    LOGGER.info("thread size - {}, resultsize - {}", totalSize, resultSet.size());
    return new ArrayList<>(resultSet);
  }

  public void internalDbToMap(List<CSVRecord> records, MovieOrRole movieOrRole) {
    if(null == records || records.isEmpty()){
      LOGGER.error("No records to consume!");
      return;
    }

    List<List<CSVRecord>> recordChunks = Util.divideAndConquer(records);
    List<CompletableFuture<Void>> results;

    switch(movieOrRole) {
      case MOVIE:
        results = sendToProcessMovie(recordChunks);
        break;
      case ROLE:
        results = sendToProcessRoles(recordChunks);
        break;
      default:
        LOGGER.error("Movie or Role to process: {} invalid!", movieOrRole.name());
        return;
    }


    var allFuturesResult = CompletableFuture.allOf(results.toArray(new CompletableFuture<?>[0]));

    try {
      allFuturesResult.get(); //waits for all to finish.
    } catch (InterruptedException | ExecutionException e){
      LOGGER.error(Constants.FAILED_THREAD, e.getMessage());
      e.printStackTrace();
    }
  }

  private List<CompletableFuture<Void>> sendToProcessMovie(List<List<CSVRecord>> recordChunks) {
    List<CompletableFuture<Void>> results = new ArrayList<>();

    for(var records : recordChunks) {
      PopulateMovies movieMapFuture = new PopulateMovies(records, masterMovieIdMap, masterMovieDataMap);
      CompletableFuture<Void> fut = CompletableFuture.supplyAsync(movieMapFuture::call, Util.threadPoolExecutor);
      results.add(fut);
    }

    return results;
  }

  private List<CompletableFuture<Void>> sendToProcessRoles(List<List<CSVRecord>> recordChunks) {
    List<CompletableFuture<Void>> results = new ArrayList<>();

    for(var records : recordChunks) {
      PopulateRoles roleMapFuture = new PopulateRoles(records, masterMovieDataMap);
      CompletableFuture<Void> fut = CompletableFuture.supplyAsync(roleMapFuture::call, Util.threadPoolExecutor);
      results.add(fut);
    }

    return results;
  }

  private List<CompletableFuture<Set<IdMapping>>> sendToProcessXbox(List<List<CSVRecord>> recordChunks) {
    List<CompletableFuture<Set<IdMapping>>> results = new ArrayList<>();

    for(var records : recordChunks) {
      MatchXbox matchXboxFuture = new MatchXbox(records, masterMovieIdMap, masterMovieDataMap);
      CompletableFuture<Set<IdMapping>> fut = CompletableFuture.supplyAsync(matchXboxFuture::call, Util.threadPoolExecutor);
      results.add(fut);
    }

    return results;
  }
}
