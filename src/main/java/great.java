
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author evans
 */
public class great {

    public static Connection con = DB.mysql();

    public static void createTables(Connection con) {

        String genre = "create table IF NOT EXISTS Genre(genreID INT AUTO_INCREMENT primary key NOT NULL, genre VARCHAR(50) unique not null)";
        String movies = "create table IF NOT EXISTS movies(movieID INT primary key NOT NULL , title VARCHAR(100) NOT NULL)";
        String moviesgenres = "create table IF NOT EXISTS moviesgenres(movieID int not null,genreID int NOT NULL,primary key(genreid, movieid), FOREIGN KEY (genreID) REFERENCES Genre(genreID), FOREIGN KEY (movieID) REFERENCES movies(movieID))";

        try {
            PreparedStatement ps = con.prepareStatement(genre);
            PreparedStatement ps1 = con.prepareStatement(movies);
            PreparedStatement ps2 = con.prepareStatement(moviesgenres);

            int item = ps.executeUpdate();
            System.out.println("created table genre >> " + item);

            int item1 = ps1.executeUpdate();
            if (item > 0) {
                System.out.println("created table  movies >> " + item1);
            } else {
                System.out.println("table movies already exists!! >> " + item1);
            }

            int item2 = ps2.executeUpdate();
            if (item2 > 0) {
                System.out.println("created table moviesgenres >> " + item2);
            } else {
                System.out.println("table moviesgenres already exists!! >> " + item2);

            }

        } catch (SQLException ex) {

            Logger.getLogger(great.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static JSONArray Url() {
        try {
            String urlString = "https://beep2.cellulant.com:9001/assessment/";
            // create the url
            URL url = new URL(urlString);
            // open the url stream, wrap it an a few "readers"
            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));

            // write the output to stdout
            String line = reader.readLine(); // json array of records
            reader.close();

            JSONArray jsonArray = new JSONArray(line);
            System.out.println("Converted object = " + jsonArray); //Outputting the result
            System.out.println("..........................................");
            return jsonArray;

        } catch (IOException ex) {
            Logger.getLogger(great.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;

    }

    public static void insertMovies(Connection con, JSONArray jsonArray) {

        for (int i = 0; i < jsonArray.length(); i++) {
            try {
                //Iterating over array

                JSONObject jsonObject = jsonArray.getJSONObject(i);

                String query = "INSERT IGNORE INTO movies(movieid, title) VALUES (?,?)";

                PreparedStatement ps = con.prepareStatement(query);
                ps.setInt(1, jsonObject.getInt("movieID"));
                ps.setString(2, jsonObject.getString("title"));

                int item = ps.executeUpdate();// movies table
                if (item > 0) {
                    System.out.println("Movie inserted >> " + item);
                } else {
                    System.out.println("Movie already exists!!"
                            + "  >> " + item);
                }
            } catch (SQLException ex) {
                Logger.getLogger(great.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    public static void insertGenres(Connection con, JSONArray jsonArray) {

        try {

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);

                String[] split = jsonObject.getString("genre").split("\\|"); // escape metacharacter

                // advanced for loop
                for (String arrayItem : split) {// iterating through genres
                    // System.out.println("Item is "+arrayItem);
                    String GenresQuery = "INSERT IGNORE INTO Genre(genre) values (?)";

                    PreparedStatement genretable = con.prepareStatement(GenresQuery);
                    genretable.setString(1, arrayItem);

                    int genre = genretable.executeUpdate();// genre table
                    if (genre > 0) {
                        System.out.println("genre data inserted >> " + genre);
                    } else {
                        System.out.println("genre data already exists!! >> " + genre);
                    }

                }

            }
        } catch (SQLException ex) {
            Logger.getLogger(great.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static Integer insertMoviesGenres(Connection con, JSONArray jsonArray) {
        Integer risultato = -1;

        try {
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                // Genre table
                String[] split = jsonObject.getString("genre").split("\\|"); // escape metacharacter
                for (String genre : split) {
                    int id = 0;

                    String gID = "SELECT (genreid) FROM Genre WHERE genre LIKE ?";
                    PreparedStatement result = con.prepareStatement(gID);
                    result.setString(1, genre);

                    ResultSet ID = result.executeQuery(); //id moviesgenres table   

                    while (ID.next()) {
                        id = ID.getInt("genreid"); // get the id
                        //System.out.println("genre id is ->> " + id);//display
                    }

                    String moviesgenres = "INSERT IGNORE INTO moviesgenres (movieid, genreid)\n"
                            + "VALUES (?,?)";

                    PreparedStatement mg = con.prepareStatement(moviesgenres, PreparedStatement.RETURN_GENERATED_KEYS);
                    mg.setInt(1, jsonObject.getInt("movieID"));
                    mg.setInt(2, id);

                    int moviegenre = mg.executeUpdate();// moviesgenres
                    ResultSet rsKeys = mg.getGeneratedKeys();

                    ResultSet rs = mg.getGeneratedKeys();
                    if (rs.next()) {
                        risultato = rs.getInt(1);
                    }
                    if (moviegenre > 0) {
                        System.out.println("moviesgenres data inserted" + moviegenre);
                    } else {
                        System.out.println("moviesgenres data already exists!! >> " + moviegenre);
                    }
                }

            }
        } catch (SQLException ex) {
            Logger.getLogger(great.class.getName()).log(Level.SEVERE, null, ex);
        }
        return risultato;

        // String count = "SELECT Genre, COUNT(movieid) AS No_of_movies FROM moviesgenres left join  Genre on  Genre.genreid = moviesgenres.genreid GROUP BY Genre"; // query getting number of movies per genre       
    }

}
