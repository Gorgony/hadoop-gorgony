package nl.hu.gorgony.bigshuf;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by njvan on 14-Jun-16.
 */
public class Analyse {
    public static void main(String[] args) throws IOException {
        Analyser analyser = new Analyser();
        analyser.init();
        String bigshufInput = FileUtils.readFileToString(new File("bigshuf.txt"));
        String[] sentences = bigshufInput.split("\\.");
        for(String sentence : sentences){
            int score = analyser.analayseSentence(sentence);
            System.out.println(score + ": " + sentence);
        }
    }
}
