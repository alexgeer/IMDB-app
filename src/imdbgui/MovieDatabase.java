/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imdbgui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Alex Geer
 */
public class MovieDatabase {

    //DB vars
    private final String dbName = "movies";
    // mySQL connection
    private Connection con;
    // connection params
    final String username = "root";  //put your username here
    final String password = "admin"; //put your password here

    //matrix collections
    ArrayList<Integer> keyword_table = new ArrayList();
    ArrayList<Integer> genre_table = new ArrayList();

    /**
     */
    public void execute(){
        //setup conn
        try {
            System.out.println("Requesting connection to mySQL... ");
            
            setupConnection();
        } catch (SQLException ex1) {
            System.out.println("Connection Failure: program terminating");
            ex1.printStackTrace();
            Logger.getLogger(MovieDatabase.class.getName()).log(Level.SEVERE, null, ex1);
            //
            System.exit(1);
        }
        
        System.out.println("Connected");

        //try to create db
        try {
            System.out.print("  Creating database \'movies\'... ");
            createDatabase();
        } catch (SQLException se) {
            System.out.println("Failed to create database, exiting");
            System.exit(0);
        }
       System.out.println(" done");

    }

    private void setupConnection() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println("Driver not found");
        }

        // connect
        con = DriverManager.getConnection("jdbc:mysql://localhost:3306/?autoReconnect=true&useSSL=false",
                username,
                password);
    }

    private void setupTables() throws SQLException{

        //import tables
        try {
            System.out.print("    importing movies_id... ");
            importMovie_id("parsed tables/movies_id.csv");
            System.out.println(" done");

            System.out.print("    importing keywords_8... ");
            importKeywords("parsed tables/keywords_8.csv");
            System.out.println(" done");

           System.out.print("    importing genres... ");
            importGenres("parsed tables/genres.csv");
            System.out.print(" done");

            System.out.print("    importing actors... ");
            importActors("parsed tables/actors.csv");
            System.out.println(" done");

            System.out.print("    importing movies... ");
            importMovies("parsed tables/movies.csv");
            System.out.println(" done");

            System.out.print("    importing directors... ");
            importDirectors("parsed tables/directors.csv");
            System.out.println("done");

            System.out.print("    importing movie_keywords... ");
            importMovie_keys("parsed tables/movID_key_8.csv");
            System.out.println("done");

            System.out.print("    importing movie_genres... ");
            importMovie_genres("parsed tables/movID_genres.csv");
            System.out.println("done");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MovieDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void createDatabase() throws SQLException {
        Statement st = con.createStatement();
        //st.execute("drop database if exists movies");
        st.execute("CREATE DATABASE IF NOT EXISTS movies DEFAULT CHARACTER SET utf8");
        st.execute("USE movies");
        st.close();

    }

    private void importMovie_id(String filename) throws FileNotFoundException, SQLException {
        PreparedStatement st = con.prepareStatement("INSERT INTO movie_id VALUES(?,?)");
        //import movies_id
        CSVReader c = new CSVReader(filename);

        ArrayList<String[]> mov_id = c.readFile();

        try {
            st.execute("CREATE TABLE movie_id (movie_id INT, name VARCHAR(255),PRIMARY KEY(movie_id) )");
        } catch (SQLException se) {
            System.out.println("cant make table : movies_id");

        }

        for (String[] sa : mov_id) {
            //unique id
            int id = Integer.parseInt(sa[0]);
            //replace all single apostrophes with double, per SQL standard
            String mov_name = sa[1];
//System.out.println("INSERT INTO movie_id VALUES(" + id + "," + mov_name + ")");
            try {
                st.setInt(1, id);
                st.setString(2, mov_name);
                st.executeUpdate();
            } catch (SQLException se) {
                System.out.println("Failing record " + sa[1]);
            }
        }
        st.close();
    }

    private void importKeywords(String filename) throws FileNotFoundException, SQLException {
        PreparedStatement st = con.prepareStatement("INSERT INTO keywords VALUES(?,?,?)");
        //import keywords
        CSVReader c = new CSVReader(filename);

        ArrayList<String[]> keywords = c.readFile();

        try {
            st.execute("CREATE TABLE keywords (keyword_id INT, keyword VARCHAR(255), freq INT, PRIMARY KEY(keyword_id) )");
        } catch (SQLException se) {
            System.out.println("cant make table : keywords");
        }

        for (String[] sa : keywords) {
            //unique id
            int id = Integer.parseInt(sa[0]);
            //replace all single apostrophes with double, per SQL standard
            String key_name = sa[1].replaceAll("\'", "\'\'");
            int freq = Integer.parseInt(sa[2]);

            //System.out.println("INSERT INTO keywords VALUES(" + id + "," + key_name + "," + freq + ")");
            try {
                st.setInt(1, id);
                st.setString(2, key_name);
                st.setInt(3, freq);
                st.executeUpdate();
            } catch (SQLException se) {
                System.out.println("Failing record " + sa[1]);
            }
        }
        st.close();
    }

    private void importGenres(String filename) throws FileNotFoundException, SQLException {
        PreparedStatement st = con.prepareStatement("INSERT INTO genres VALUES(?,?)");
        //import keywords
        CSVReader c = new CSVReader(filename);

        ArrayList<String[]> genres = c.readFile();

        try {
            st.execute("CREATE TABLE genres (genre_id INT, genre VARCHAR(255), PRIMARY KEY(genre_id) )");
        } catch (SQLException se) {
            System.out.println("cant make table : genres");
        }

        for (String[] sa : genres) {
            //unique id
            int id = Integer.parseInt(sa[0]);
            //replace all single apostrophes with double, per SQL standard
            String genre = sa[1].replaceAll("\'", "\'\'");

            //System.out.println("INSERT INTO genres VALUES(" + id + "," + genre + ")");
            try {
                st.setInt(1, id);
                st.setString(2, genre);
                st.executeUpdate();
            } catch (SQLException se) {
                System.out.println("Failing record " + sa[1]);
            }
        }
        st.close();
    }

    private void importActors(String filename) throws FileNotFoundException, SQLException {
        PreparedStatement st = con.prepareStatement("INSERT INTO actors VALUES(?,?,?)");
        //import keywords
        CSVReader c = new CSVReader(filename);

        ArrayList<String[]> actors = c.readFile();

        try {
            st.execute("CREATE TABLE IF NOT EXISTS actors (movie_id INT, name VARCHAR(255), character_name VARCHAR(255), "
                    + "FOREIGN KEY (movie_id) REFERENCES movie_id (movie_id), "
                    + "CONSTRAINT pk_actors PRIMARY KEY(movie_id, name , character_name) )");

        } catch (SQLException se) {
            System.out.println("can't make table : actors... exiting");
            se.printStackTrace();
            System.exit(0);
        }

        for (String[] sa : actors) {
            //unique id

            int mov_id = Integer.parseInt(sa[0]);
            //replace all single apostrophes with double, per SQL standard
            String name = sa[1].replaceAll("\'", "\'\'");
            String character;
            try {
                character = sa[2].replaceAll("\'", "\'\'");
            } catch (ArrayIndexOutOfBoundsException np) {
                System.out.println(sa[1] + " none");
                character = "none";
            }

            try {
                st.setInt(1, mov_id);
                st.setString(2, name);
                st.setString(3, character);
                st.executeUpdate();
            } catch (com.mysql.jdbc.MysqlDataTruncation dt) {
                st.setInt(1, mov_id);
                st.setString(2, name);
                st.setString(3, character.substring(0, 255));
                st.executeUpdate();
            } catch (SQLException se) {
                System.out.println("Failing record " + sa[1]);
                se.printStackTrace();
            }
        }
        st.close();
    }

    private void importDirectors(String filename) throws FileNotFoundException, SQLException {
        PreparedStatement st = con.prepareStatement("INSERT INTO directors VALUES(?,?)");
        //import keywords
        CSVReader c = new CSVReader(filename);

        ArrayList<String[]> genres = c.readFile();

        try {
            st.execute("CREATE TABLE IF NOT EXISTS directors (mov_id INT, director VARCHAR(255),"
                    + "FOREIGN KEY (mov_id) REFERENCES movie_id (movie_id),"
                    + " CONSTRAINT pk_dir PRIMARY KEY(mov_id, director) )");
        } catch (SQLException se) {
            se.printStackTrace();
            System.out.println("can't make table : directors");
            System.exit(0);
        }

        for (String[] sa : genres) {
            //unique id
            int id = Integer.parseInt(sa[0]);
            //replace all single apostrophes with double, per SQL standard
            String director = sa[1].replaceAll("\'", "\'\'");

            //System.out.println("INSERT INTO genres VALUES(" + id + "," + genre + ")");
            try {
                st.setInt(1, id);
                st.setString(2, director);
                st.executeUpdate();
            } catch (SQLException se) {
                System.out.println("Failing record " + sa[1]);
                se.printStackTrace();
            }
        }
        st.close();
    }

    private void importMovie_keys(String filename) throws FileNotFoundException, SQLException {
        PreparedStatement st = con.prepareStatement("INSERT INTO movie_keys VALUES(?,?)");
        //import keywords
        CSVReader c = new CSVReader(filename);

        ArrayList<String[]> genres = c.readFile();

        try {
            st.execute("CREATE TABLE movie_keys (movie_id INT, keyword_id INT, "
                    + "FOREIGN KEY (movie_id) REFERENCES movie_id(movie_id),"
                    + "FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id),"
                    + "CONSTRAINT pk_movKey PRIMARY KEY(movie_id, keyword_id) )");
        } catch (SQLException se) {
            System.out.println("cant make table : movie_keys");
        }

        for (String[] sa : genres) {
            //unique id
            int id = Integer.parseInt(sa[0]);
            //replace all single apostrophes with double, per SQL standard
            String keyword_id = sa[1].replaceAll("\'", "\'\'");

            //System.out.println("INSERT INTO genres VALUES(" + id + "," + genre + ")");
            try {
                st.setInt(1, id);
                st.setString(2, keyword_id);
                st.executeUpdate();
            } catch (SQLException se) {
                System.out.println("Failing record " + sa[1]);
            }
        }
        st.close();
    }

    private void importMovie_genres(String filename) throws FileNotFoundException, SQLException {
        PreparedStatement st = con.prepareStatement("INSERT INTO movie_genres VALUES(?,?)");
        //import keywords
        CSVReader c = new CSVReader(filename);

        ArrayList<String[]> genres = c.readFile();

        try {
            st.execute("CREATE TABLE movie_genres (movie_id INT, genre_id INT, "
                    + "FOREIGN KEY (movie_id) REFERENCES movie_id(movie_id),"
                    + "FOREIGN KEY (genre_id) REFERENCES genres(genre_id),"
                    + "CONSTRAINT pk_movKey PRIMARY KEY(movie_id, genre_id) )");
        } catch (SQLException se) {
            System.out.println("cant make table : genres");
            System.exit(0);
        }

        for (String[] sa : genres) {
            //unique id
            int id = Integer.parseInt(sa[0]);
            //replace all single apostrophes with double, per SQL standard
            String genre = sa[1].replaceAll("\'", "\'\'");

            //System.out.println("INSERT INTO genres VALUES(" + id + "," + genre + ")");
            try {
                st.setInt(1, id);
                st.setString(2, genre);
                st.executeUpdate();
            } catch (SQLException se) {
                System.out.println("Failing record " + sa[1]);
            }
        }
        st.close();
    }

    private void importMovies(String filename) throws FileNotFoundException, SQLException {
        PreparedStatement st = con.prepareStatement("INSERT INTO movies VALUES(?,?,?,?,?,?)");
        //import movies_id
        CSVReader c = new CSVReader(filename);

        ArrayList<String[]> mov_id = c.readFile();

        try {
            st.execute("CREATE TABLE IF NOT EXISTS movies(movie_id INT, overview TEXT, release_date DATE, runtime INT, vote_avg FLOAT, vote_count INT, PRIMARY KEY(movie_id) )");
        } catch (SQLException se) {
            se.printStackTrace();
            System.out.println("cant make table : movies");
            System.exit(0);
        }

        for (String[] sa : mov_id) {
            int id = 0;
            String overview = null;
            String[] r_date = null;
            int runtime = 0;
            float vote_avg = 0;
            int vote_count = 0;
            Date release_date = new Date(1990, 2, 3);
            try {
                //unique id
                id = Integer.parseInt(sa[0]);
                overview = sa[1];
                r_date = sa[2].split("/");
                release_date = Date.valueOf(Integer.parseInt(r_date[2]) + "-" + Integer.parseInt(r_date[0]) + "-" + Integer.parseInt(r_date[1]));

                runtime = Integer.parseInt(sa[3].replaceAll("[.].", ""));
                vote_avg = Float.parseFloat(sa[4]);
                vote_count = Integer.parseInt(sa[5]);
            } catch (NumberFormatException numberFormatException) {
                int i = 0;
                for (String s : sa) {
                    System.out.println(i++ + " " + s);
                }
            }
//System.out.println("INSERT INTO movie_id VALUES(" + id + "," + mov_name + ")");
            try {
                st.setInt(1, id);
                st.setString(2, overview);
                st.setDate(3, release_date);
                st.setInt(4, runtime);
                st.setFloat(5, vote_avg);
                st.setInt(6, vote_count);

                st.executeUpdate();
            } catch (SQLException se) {
                System.out.println("Failing record " + sa[1]);
            } catch (IllegalArgumentException iae) {
                iae.printStackTrace();
                System.out.println(r_date);
                System.exit(0);
            } catch (ArrayIndexOutOfBoundsException oob) {
                System.out.println(sa[0]);
            }
        }
        st.close();
    }

    private void buildSimTableCreateStmt() throws SQLException {
        String sql;
        Statement st = con.createStatement();

        String createTable = "CREATE TABLE IF NOT EXISTS similarity_matrix(movie_id INT, director VARCHAR(255),";

        //grab and append keywords
        sql = "SELECT keyword_id from keywords";
        ResultSet keywords = st.executeQuery(sql);
        int keyID;
        while (keywords.next()) {
            keyID = keywords.getInt(1);
            createTable += "k" + keyID + " INT,";
            keyword_table.add(keyID);
        }

        //grab and append genres
        sql = "SELECT genre_id from genres";
        ResultSet genres = st.executeQuery(sql);
        int genreID;
        while (genres.next()) {
            genreID = genres.getInt(1);
            createTable += "g" + genreID + " INT,";
            genre_table.add(genreID);
        }

        createTable += "FOREIGN KEY (movie_id) REFERENCES movie_id(movie_id), PRIMARY KEY(movie_id) )";

        st.execute(createTable);
        st.close();
    }

    private void buildSimMatrix() throws SQLException {
        String sql;
        Statement st = con.createStatement();
        Statement updateSt = con.createStatement();
        //build columns
        buildSimTableCreateStmt();

        //do insertions
        sql = "SELECT movie_id FROM movie_id";
        ResultSet prefix = st.executeQuery(sql);
        
        //build statement
        String update;
        int movie_id;
        int last_movie = -1;
        String director;

        while (prefix.next()) {
            //add ID
            update = "INSERT INTO similarity_matrix VALUES(";
            while ((movie_id = prefix.getInt("movie_id")) == last_movie) {
                prefix.next();
            }

            update += movie_id + ",";

            //get director
            try (Statement st2 = con.createStatement()) {
                ResultSet directors = st2.executeQuery("SELECT director FROM directors WHERE mov_id = " + movie_id);
                if (directors.next()) {
                    director = "\'" + directors.getString("director").replaceAll("\'", "\'\'") + "\'";
                } else {
                    director = null;
                }
                update += director + ",";
            } catch (SQLException se) {
                se.printStackTrace();
                System.out.println("couldnt get directors for " + movie_id);
            }

            int keyword;
            //get and process keywords
            try (Statement st2 = con.createStatement()) {
                ResultSet mov_keys = st2.executeQuery("SELECT keyword_id FROM movie_keys WHERE movie_id = " + movie_id);

                int curr_key;
                ArrayList<Integer> current_movie_keys = new ArrayList();
                while (mov_keys.next()) {
                    current_movie_keys.add(mov_keys.getInt(1));
                }

                //outerloop through table
                for (Integer table_key : keyword_table) {
                    boolean matched = false;
                    //inner loop through record
                    for (Integer current_key : current_movie_keys) {
                        if (table_key.intValue() == current_key.intValue()) {
                            matched = true;
                        }
                    }
                    if (matched) {
                        update += "1,";
                    } else {
                        update += "0,";
                    }

                }
            } catch (SQLException se) {
                se.printStackTrace();
                System.out.println("couldnt get keywords for " + movie_id);
            }

            //get and process keywords
            try (Statement st2 = con.createStatement()) {
                ResultSet mov_gen = st2.executeQuery("SELECT genre_id FROM movie_genres WHERE movie_id = " + movie_id);

                int curr_key;
                ArrayList<Integer> current_movie_genres = new ArrayList();
                while (mov_gen.next()) {
                    current_movie_genres.add(mov_gen.getInt(1));
                }

                //outerloop through table
                for (Integer table_gen : genre_table) {
                    boolean matched = false;
                    //inner loop through record
                    for (Integer current_gen : current_movie_genres) {
                        if (table_gen.intValue() == current_gen.intValue()) {
                            matched = true;
                        }
                    }
                    if (matched) {
                        update += "1,";
                    } else {
                        update += "0,";
                    }

                }
            } catch (SQLException se) {
                se.printStackTrace();
                System.out.println("couldnt get genres for " + movie_id);
            }

            update = update.substring(0, update.length() - 1) + ")";


            updateSt.executeUpdate(update);
            last_movie = movie_id;
        }
        st.close();
        updateSt.close();

    }

    public String searchForLike(String movie) {
        String result = "error";
        try {
            Statement st = con.createStatement();
            movie = movie.replaceAll("\'", "\'\'");
            String sql = "SELECT * FROM movie_id I INNER JOIN movies M ON M.movie_id = I.movie_ID WHERE name = '" + movie + "'"
                    + "UNION SELECT * FROM movie_id I2 INNER JOIN movies M2 ON M2.movie_id = I2.movie_ID WHERE name LIKE '%" + movie + "%'";

            ResultSet search = st.executeQuery(sql);
            result = "";
            int i = 0;

            result += "RESULTS**********\n";
            result += "===================\n";
            while (search.next() && i++ < 5) {

                result += search.getString("name") + " (" + search.getDate("release_date").toString().substring(0,4) + ")";
                result += "\n" + search.getString("overview") + "\n";
                result += getDirector(search.getInt("movie_id"));
                result += getActors(search.getInt("movie_id"));
                result += ("\n" + "===================\n\n");

            }
        } catch (SQLException se) {
            System.out.println("search failed");
            se.printStackTrace();
        }
        return result;
    }

    public String searchFor(String movie) {
        String result = "error";
        try {
            Statement st = con.createStatement();
            movie = movie.replaceAll("\'", "\'\'");
            String sql = "SELECT * FROM movie_id I INNER JOIN movies M ON M.movie_id = I.movie_ID WHERE name = '" + movie + "'";

            ResultSet search = st.executeQuery(sql);
            result = "";
            int i = 0;

            result += "===================\n";
            while (search.next() && i++ < 5) {

                result += search.getString("name") + "(" + search.getDate("release_date").toString() + ")";
                result += "\n" + search.getString("overview");

                result += ("\n" + "===================");

            }
        } catch (SQLException se) {
            System.out.println("search failed");
            se.printStackTrace();
        }
        return result;
    }

    public String recFor(String movie) {
        int mov_id;
        try {
            Statement st = con.createStatement();
            movie = movie.replaceAll("\'", "\'\'");
            String sql = "SELECT movie_id FROM movie_id WHERE name = '" + movie + "'";
            ResultSet rs = st.executeQuery(sql);

            if (rs.next()) {
                mov_id = rs.getInt("movie_id");
            } else {
                return "movie not found";
            }

            //get top5 most similar (keyword and genre)
            List<Pair> top5 = generateRecommendations(mov_id);

            //sort by popularity
            top5 = sortPopularity(top5);

            String result = "";
            sql = "SELECT * FROM movie_id I JOIN movies M on M.movie_id = I.movie_id WHERE M.movie_id = ";
            String actor_sql = "SELECT name, character_name FROM actors WHERE movie_id = ";
            int recno;

            for (Pair p : top5) {
                rs = st.executeQuery(sql + p.mov_id);
                rs.next();
                recno = (top5.indexOf(p) + 1);
                result += recno + ". " + rs.getString("name") + " (" + rs.getDate("release_date").toString().substring(0,4) + ")\n"
                        + "popularity index: " + p.popularity + "    distance: " + p.distance + "\n"
                        //+ "\n" + rs.getString("overview")
                        + "\n\n";
                //add directors and actors
                //result += getDirector(p.mov_id);
                //result += getActors(p.mov_id) + "\n===============\n\n";

            }
            return result;
        } catch (SQLException ex) {
            Logger.getLogger(MovieDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }

        return "error";
    }

    private String getDirector(int mov_id) throws SQLException {
        String dir_sql = "SELECT director FROM directors WHERE mov_id = ";
        Statement st = con.createStatement();
        String result;

        //query
        ResultSet rs = st.executeQuery(dir_sql + mov_id);
        result = "Directed by:\n";
        while (rs.next()) {
            //append to result
            result += "\t" + rs.getString(1) + "\n";
        }
        
        //close and append
        st.close();
        return result;

    }

    private String getActors(int mov_id) throws SQLException {
        String actor_sql = "SELECT name, character_name FROM actors WHERE movie_id = ";
        Statement st = con.createStatement();
        String result;
        
        //query
        ResultSet rs = st.executeQuery(actor_sql + mov_id);
        result = "Starring :\n";
        while (rs.next()) {
            //append result
            result += "\t" + rs.getString(1) + " as " + rs.getString(2) + "\n";
        }
        
        //close and return
        st.close();
        return result;

    }

    private List<Pair> generateRecommendations(int mov_id) {
        ArrayList<Pair> top5;
        try {
            Statement st = con.createStatement();
            Statement st2 = con.createStatement();

            ResultSet mov = st.executeQuery("SELECT * from similarity_matrix where movie_id = " + mov_id);
            ResultSet matrix = st2.executeQuery("SELECT * from similarity_matrix WHERE movie_id != " + mov_id);

            ArrayList<Pair> distances = new ArrayList();

            ResultSetMetaData metadata = matrix.getMetaData();
            int columnCount = metadata.getColumnCount();

            String director = "";

            if (mov.next()) {
                String other_director;
                while (matrix.next()) {
                    int sum = 0;
                    other_director = matrix.getString("director");

                    for (int i = 3; i < columnCount; i++) {
                        sum += Math.pow(mov.getInt(i) - matrix.getInt(i), 2);
                    }
                    //scale for director
                    double new_sum = sum;
                    if (director.equals(other_director)) {
                        new_sum *= .5;
                    }
                    Pair p = new Pair(matrix.getInt("movie_id"), Math.sqrt(new_sum));
                    distances.add(p);
                    //System.out.println(p.mov_id + ", " + p.distance);
                }
                matrix.close();
                mov.close();
                st.close();
                st2.close();

                //sort
                Collections.sort(distances, (Pair p1, Pair p2) -> {
                    if (p1.distance == p2.distance) {
                        return 0;
                    } else if (p1.distance < p2.distance) {
                        return -1;
                    } else if (p1.distance > p2.distance) {
                        return 1;
                    } else {
                        return 0;
                    }
                });

                // sort top 20 by popularity
                for (int i = 0; i < 20; i++) {
                    Pair p = distances.get(i);
                    System.out.println(p.mov_id + ", " + p.distance);
                }
            }

            return distances.subList(0, 5);

        } catch (SQLException ex) {
            Logger.getLogger(MovieDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    private List<Pair> sortPopularity(List<Pair> top5) {

        try {
            Statement st = con.createStatement();
            String sql = "SELECT movie_id,(vote_avg*vote_avg*vote_count/(SELECT MAX(vote_count) FROM movies)) AS popularity FROM movies WHERE movie_id = ";

            for (Pair p : top5) {
                sql += p.mov_id + " OR movie_id = ";
            }

            sql = sql.substring(0, sql.length() - " OR movie_id = ".length()) + " ORDER BY popularity";

            ResultSet sorted = st.executeQuery(sql);

            while (sorted.next()) {
                for (Pair p : top5) {
                    if (p.mov_id == sorted.getInt("movie_id")) {
                        p.setPopularity(sorted.getDouble("popularity"));
                    }
                }
                
                
                //sort
                Collections.sort(top5, (Pair p1, Pair p2) -> {
                    if (p1.popularity == p2.popularity) {
                        return 0;
                    } else if (p1.popularity < p2.popularity) {
                        return 1;
                    } else if (p1.popularity > p2.popularity) {
                        return -1;
                    } else {
                        return 0;
                    }
                });

            }
            //close statement
                st.close();
        } catch (SQLException ex) {
            System.out.println("couldn't sort on popularity");
            ex.printStackTrace();
            return top5;
        }
        
        
        return top5;
    }

}

class Pair {

    int mov_id;
    double distance;
    double popularity;

    public void setPopularity(double popularity) {
        this.popularity = popularity;
    }

    public Pair(int mov_id, double distance) {
        this.mov_id = mov_id;
        this.distance = distance;
    }

}

class CSVReader {

    private final File inFile;
    private final BufferedReader reader;

    public CSVReader(String filename) throws FileNotFoundException {
        inFile = new File(filename);
        reader = new BufferedReader(new FileReader(inFile));
    }

    public String[] readLine() throws IOException {
        String line = reader.readLine();
        if (line != null) {
            return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        }

        return null;
    }

    public ArrayList<String[]> readFile() {
        ArrayList<String[]> csvArr = new ArrayList();
        String[] line = {""};

        try {
            while ((line = this.readLine()) != null) {
                csvArr.add(line);
            }
        } catch (IOException ex) {
            Logger.getLogger(CSVReader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return csvArr;
    }
}
