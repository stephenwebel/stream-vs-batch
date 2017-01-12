package com.webel.stream;

import com.webel.common.Song;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.util.StopWatch;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by swebel on 1/10/2017.
 */
@Slf4j
public class Main {

    public static void main(String[] args) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        //MySQL database we are using
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/");//change url
        dataSource.setUsername("java_owner");//change userid
        dataSource.setPassword("java_pw");//change pwd

        NamedParameterJdbcTemplate namedTemplate = new NamedParameterJdbcTemplate(dataSource);

        StreamTestRunner streamTestRunner = new StreamTestRunner(namedTemplate);
        streamTestRunner.runStreamTest();
    }
}
