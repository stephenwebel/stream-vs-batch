package com.webel.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by stephenwebel1 on 1/11/17.
 */
@Slf4j
class StreamFileWriter implements AutoCloseable {

    private static int SIZE = 0;
    private static final int BATCH_SIZE = 10;
    private static final String INSERT_SONG = "insert " +
            "into music_library.songz " +
            "(artist, title, link, lyrics) " +
            "values " +
            "(:artist, :title, :link, :lyrics)";
    private final FileWriter batchWriter;
    private final FileWriter writer;
    private final FileWriter volatileWriter;
    private final NamedParameterJdbcTemplate namedTemplate;
    private ObjectMapper objectMapper = new ObjectMapper();

    private StringBuilder batch = new StringBuilder();

    public StreamFileWriter(String pathToSongJson, NamedParameterJdbcTemplate namedTemplate) throws IOException {
        writer = new FileWriter(pathToSongJson, true);
        batchWriter = new FileWriter(pathToSongJson, true);
        volatileWriter = new FileWriter(pathToSongJson, true);
        this.namedTemplate = namedTemplate;
    }

    public void insertSong(Song song) {
        Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("artist", song.getArtistName());
        parameters.put("title", song.getTitle());
        parameters.put("link", song.getLink());
        parameters.put("lyrics", song.getLyrics());
        namedTemplate.update(INSERT_SONG, parameters);
    }

    private List<Map<String, Object>> parameters = Lists.newArrayList();

    public void batchInsertSong(Song song) {

    }

    /**
     * Writes a song to the file.
     *
     * @param song
     */
    void writeSong(Song song) {
        try {
            writer.write(objectMapper.writeValueAsString(song) + "\n");
        } catch (IOException e) {
            log.warn("could not write song: {}", song);
        }
    }

    void batchWrite(Song song) {
        try {
            ++SIZE;
            //this is dumb
            batch.append(objectMapper.writeValueAsString(song)).append("\n");
            if (SIZE % BATCH_SIZE == 0) {
                batchWriter.write(batch.toString());
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    Song volatileWrite(Song song) {
        try {
            volatileWriter.write(objectMapper.writeValueAsString(song) + "\n");
            return song;
        } catch (IOException e) {
            log.warn("could not write song: {}", song);
        }
        return null;
    }

    public void close() throws IOException {
        writer.close();
        batchWriter.close();
        volatileWriter.close();
    }
}