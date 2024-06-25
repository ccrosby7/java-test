package com.real.matcher.util;

import com.real.matcher.Matcher.CsvStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


//Note: Both of these pull the entire data into memory to avoid choking on timeouts or blocking I/O
public class Util {
    public static final int MAX_THREADS = 100;
    public static final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(MAX_THREADS, MAX_THREADS,
                10, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
    private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);
    private static final CSVFormat format =
            CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setTrim(true)
                    .build();

    private Util(){

    }

    public static CSVParser getStreamData(CsvStream stream, String streamName){
        try {
            Stream<String> lines = Stream.concat(stream.getHeaderRow().lines(), stream.getDataRows()); //recombine this for csvparser
            Reader in = new StringReader(lines.collect(Collectors.joining("\n")));
            lines.close();
            return new CSVParser(in, format);
        } catch (IOException ex) {
            LOGGER.debug(ex.getMessage());
            LOGGER.error("ERROR - stream {} failed to load!", streamName);
            return null;
        }
    }

    public static List<List<CSVRecord>> divideAndConquer(List<CSVRecord> records) {
        int totalRecords = records.size();
        int chunkSize = totalRecords/MAX_THREADS;
        int i = 0;

        if(chunkSize < MAX_THREADS) {
            chunkSize = MAX_THREADS;
        }

        ArrayList<List<CSVRecord>> recordsToProcess = new ArrayList<>(chunkSize);

        while (i < totalRecords) {
            if( i + chunkSize < totalRecords) {
                recordsToProcess.add(records.subList(i, i + chunkSize));
            } else {
                recordsToProcess.add(records.subList(i, records.size()));
            }
            i += chunkSize;
        }

        return recordsToProcess;
    }

}
