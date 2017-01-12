package com.webel.batch;

import com.google.common.collect.Lists;
import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static com.webel.common.SongArgumentParser.parseArgumentLine;

/**
 * Created by swebel on 1/10/2017.
 */
@Slf4j
public class SongFileReader {

    private static final String LYRIC_BLOCK_TERMINATOR = "\"";

    static List<Song> readSongsFromFile(String pathToSongData) {
        List<Song> songs = Lists.newArrayList();
        try (BufferedReader songFileReader = new BufferedReader(new FileReader(pathToSongData))) {
            String currentLine = songFileReader.readLine();
            log.info("Headers for song data: {}", currentLine);

            boolean inLyricBlock = false;
            StringBuilder lyricBuilder = new StringBuilder();
            Song.SongBuilder currentSongBuilder = Song.builder();
            while ((currentLine = songFileReader.readLine()) != null) {
                if (inLyricBlock) {
                    lyricBuilder.append(currentLine);
                    if (LYRIC_BLOCK_TERMINATOR.equals(currentLine)) {
                        //add the completed song
                        songs.add(currentSongBuilder.lyrics(lyricBuilder.toString()).build());
                        inLyricBlock = false;
                    }
                } else {
                    List<String> arguments = parseArgumentLine(currentLine);
                    currentSongBuilder = Song.builder()
                            .artistName(arguments.get(0))
                            .title(arguments.get(1))
                            .link(arguments.get(2));
                    lyricBuilder = new StringBuilder(arguments.get(3));
                    inLyricBlock = true;
                }
            }
        } catch (IOException e) {
            log.error("Couldn't open song file from path: {}", pathToSongData, e);
        }
        return songs;
    }
}
