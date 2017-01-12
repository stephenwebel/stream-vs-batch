package com.webel.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.StopWatch;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by stephenwebel1 on 1/12/17.
 */
@Slf4j
public class BatchTestRunner {
    //    private static final String pathToSongData = "src/main/resources/songdata.csv";
//    private static final String pathToSongData = "src/main/resources/song.txt";
    private static final String pathToSongData = "src/main/resources/big_songs.txt";
    private static final String pathToSongJson = "src/main/resources/pureBatchWrite.json";
    private static final StopWatch stopWatch = new StopWatch();
    private ObjectMapper objectMapper = new ObjectMapper();
    private static final String INSERT_SONG = "insert " +
            "into music_library.songz " +
            "(artist, title, link, lyrics) " +
            "values " +
            "(:artist, :title, :link, :lyrics)";
    private final NamedParameterJdbcTemplate namedTemplate;


    public BatchTestRunner(NamedParameterJdbcTemplate namedTemplate) throws IOException {
        this.namedTemplate = namedTemplate;
    }

    void runTest() {
//        runBatchReadTest();
//        runBatchReadFileWriteTest();
        runBatchReadDbWriteTest();
    }

    private void runBatchReadDbWriteTest() {
        log.info("Starting Batch Processing");
        stopWatch.start("Batch File Read -> Batch Write");
        //load all songs into memory
        Collection<Song> songs = new SongFileReader(pathToSongData).readSongsFromFile();
        log.info("found {} songs", songs.size());
//        writeSongs(songs);
        writeSongsAsBatch(songs);
        stopWatch.stop();
        log.info(stopWatch.prettyPrint());
    }

    void writeSongs(Collection<Song> songs) {
        log.info("writing songs to db");
        for (Song song : songs) {
            Map<String, Object> parameterMap = Maps.newHashMap();
            parameterMap.put("artist", song.getArtistName());
            parameterMap.put("title", song.getTitle());
            parameterMap.put("link", song.getLink());
            parameterMap.put("lyrics", song.getLyrics());
            namedTemplate.update(INSERT_SONG, parameterMap);
        }
    }

    void writeSongsAsBatch(Collection<Song> songs) {
        log.info("preparing songs for database write");
        List<Map<String, Object>> parameters = Lists.newArrayList();
        for (Song song : songs) {
            Map<String, Object> parameterMap = Maps.newHashMap();
            parameterMap.put("artist", song.getArtistName());
            parameterMap.put("title", song.getTitle());
            parameterMap.put("link", song.getLink());
            parameterMap.put("lyrics", song.getLyrics());
            parameters.add(parameterMap);
        }
        log.info("writing batch to database");
        namedTemplate.batchUpdate(INSERT_SONG, parameters.toArray(new Map[songs.size()]));

    }

    public void runBatchReadTest() {
        log.info("Starting Batch Read Test");
        stopWatch.start("Batch File Read");
        //load all songs into memory
        List<Song> songs = new SongFileReader(pathToSongData).readSongsFromFile();
        log.info("read {} songs from {}", songs.size(), pathToSongData);
        log.info("first song: {}", songs.get(0));
        log.info("last song: {}", songs.get(songs.size() - 1));
        stopWatch.stop();
        log.info("{}", stopWatch.prettyPrint());
    }

    public void runBatchReadFileWriteTest() {
        log.info("Starting Batch Processing");
        stopWatch.start("Batch File Read -> Batch Write");
        //load all songs into memory
        Collection<Song> songs = new SongFileReader(pathToSongData).readSongsFromFile();
        log.info("found {} songs", songs.size());
        //create the full file to write
        StringBuilder songString = new StringBuilder();
        for (Song song : songs) {
            try {
                songString.append(objectMapper.writeValueAsString(song)).append("\n");
            } catch (JsonProcessingException e) {
                log.error("", e);
            }
        }
        //write the string
        try (FileWriter writer = new FileWriter(pathToSongJson, false)) {
            writer.write(songString.toString());
        } catch (IOException e) {
            log.error("", e);
        }
        stopWatch.stop();
        log.info(stopWatch.prettyPrint());
    }
}
