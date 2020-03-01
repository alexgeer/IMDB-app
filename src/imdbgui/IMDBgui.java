/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imdbgui;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.application.Application;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.ScrollPane.ScrollBarPolicy;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextAreaBuilder;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;
import javafx.stage.Stage;

/**
 *
 * @author Alex Geer
 */
public class IMDBgui extends Application {

    //text display
    private TextArea output;
    //client controls
    private LinkedList<Button> buttons;
    private MovieDatabase mdb;

    @Override
    public void start(Stage primaryStage) {
        MovieDatabase mdb = new MovieDatabase();
        //setup connection and tables
        mdb.execute();
        BorderPane root = new BorderPane();
        
        
       
        
        Label label1 = new Label("Movie: ");
        TextField searchBar = new TextField();
        HBox hb = new HBox();
        Button submit = new Button("Search");
        Button rec = new Button("Recommendation");
        //Set an action for the search button
        submit.setOnAction(new EventHandler<ActionEvent>() {

            @Override
            public void handle(ActionEvent e) {
                if ((searchBar.getText() != null && !searchBar.getText().isEmpty())) {
                    output.clear();
                    output.appendText(mdb.searchForLike(searchBar.getText()));
                } else {
                    output.clear();
                    output.appendText("No search specified");
                }
            }
        });
        rec.setOnAction((ActionEvent e) -> {
            if ((searchBar.getText() != null && !searchBar.getText().isEmpty())) {
                output.clear();
                output.appendText("Generating recommendation... \n");
                output.appendText(mdb.recFor(searchBar.getText()));
            } else {
                output.clear();
                output.appendText("No search specified");
            }
        });
        
        
        hb.getChildren().addAll(label1, searchBar, submit, rec);
        
        root.setTop(hb);

        root.setCenter(addScroll());
        Scene scene = new Scene(root, 400, 600);

        //show window       
        primaryStage.setTitle("Movie Database");
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private ScrollPane addScroll() {
        ScrollPane sp = new ScrollPane();
        sp.setHbarPolicy(ScrollBarPolicy.NEVER);
        sp.setVbarPolicy(ScrollBarPolicy.ALWAYS);
        //set text output 
        output = new TextArea();
        output.setEditable(false);
        output.setWrapText(true);

        //set borders
        sp.setStyle("-fx-border-color: black;");
        //place output in scroll
        sp.setContent(output);
        sp.setFitToHeight(true);

        return sp;
    }

    public static void main(String[] args) {
        launch(args);
    }

    private void setSearchOption(String string) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private Node addSearchBar() {
        Label label1 = new Label("Movie: ");
        TextField searchBar = new TextField();
        HBox hb = new HBox();
        Button submit = new Button("Submit");
        //Setting an action for the Submit button
        submit.setOnAction((ActionEvent e) -> {
            if ((searchBar.getText() != null && !searchBar.getText().isEmpty())) {
                
            } else {
                
            }
        });
        hb.getChildren().addAll(label1, searchBar, submit);
        hb.setSpacing(10);

        return hb;
    }
    

}




