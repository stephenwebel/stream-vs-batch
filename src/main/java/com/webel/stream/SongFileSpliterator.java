package com.webel.stream;

import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Spliterators;
import java.util.function.Consumer;

import static com.webel.common.SongArgumentParser.parseArgumentLine;

/**
 * Created by swebel on 1/10/2017.
 */
@Slf4j
public class SongFileSpliterator extends Spliterators.AbstractSpliterator<Song> {
    private static final String LYRIC_BLOCK_TERMINATOR = "\"";

    private final BufferedReader songFileReader;

    /**
     * Creates a spliterator reporting the given estimated size and
     * additionalCharacteristics.
     */
    protected SongFileSpliterator(BufferedReader songFileReader) {
        super(57650, 0);
        this.songFileReader = songFileReader;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Song> action) {
        Song.SongBuilder songBuilder = Song.builder();
        if (buildSong(songBuilder)) {
            action.accept(songBuilder.build());
            return true;
        } else {
            return false;
        }
    }

    private boolean buildSong(Song.SongBuilder songBuilder) {
        try {
            String startingLine = songFileReader.readLine();
            if (startingLine == null) {
                return false;
            }
            List<String> songArgs = parseArgumentLine(startingLine);
            songBuilder.artistName(songArgs.get(0))
                    .title(songArgs.get(1))
                    .link(songArgs.get(2));

            StringBuilder lyricBuilder = new StringBuilder(songArgs.get(3));
            while (true) {
                String nextLyrics = songFileReader.readLine();
                if (nextLyrics == null || LYRIC_BLOCK_TERMINATOR.equals(nextLyrics)) {
                    break;
                } else {
                    lyricBuilder.append(nextLyrics);
                }
            }
            songBuilder.lyrics(lyricBuilder.toString());
            return true;
        } catch (IOException e) {
            log.error("Failed to build next song!", e);
        }
        return false;
    }
}
