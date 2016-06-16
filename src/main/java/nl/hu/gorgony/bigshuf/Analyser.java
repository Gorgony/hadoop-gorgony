package nl.hu.gorgony.bigshuf;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by njvan on 14-Jun-16.
 */
public class Analyser {
    HashMap<String, Integer> startsWith = new HashMap<String, Integer>();
    HashMap<String, Integer> endsWith = new HashMap<String, Integer>();
    HashMap<String, Integer> combinations = new HashMap<String, Integer>();
    HashMap<String, Integer> letterFrequency = new HashMap<String, Integer>();
    int vowelPercentage = 0;

    public Analyser(){}

    public void init() throws IOException {
        //Read files
        String combinationCountFile = FileUtils.readFileToString(new File("english/CombinationCount.lanstats"));
        String vowelCountfile = FileUtils.readFileToString(new File("english/VowelCount.lanstats"));
        String startEndCountFile = FileUtils.readFileToString(new File("english/StartEndCount.lanstats"));
        String letterCountFile = FileUtils.readFileToString(new File("english/LetterCount.lanstats"));

        //Set combination hashmap
        int combinationTotal = 0;

        String[] lines = combinationCountFile.split("\\n");
        for(String line : lines){
            String[] parts =  line.split("\\s");
            int value = Integer.parseInt(parts[1]);
            combinations.put(parts[0],value);
            combinationTotal += value;
        }
        setScoreInHashmap(combinations,combinationTotal);

        //Set vowelpercentage
        String[] parts = vowelCountfile.split("\\s");
        vowelPercentage = Integer.parseInt(parts[1]);
//        System.out.println(vowelPercentage);

        //Set startsWith & endsWith hashmaps
        int startTotal = 0;
        int endTotal = 0;

        lines = startEndCountFile.split("\\n");
        for(String line : lines){
            parts =  line.split("\\s");
            String[] identifier = parts[0].split(":");
            int value = Integer.parseInt(parts[1]);
            if(identifier[0].equals("start")){
                startsWith.put(identifier[1],value);
                startTotal += value;
            } else if((identifier[0].equals("end"))){
                endsWith.put(identifier[1],value);
                endTotal += value;
            }
        }

        setScoreInHashmap(startsWith,startTotal);
        setScoreInHashmap(endsWith,endTotal);

        //Set letterfrequency hashmap
        int letterTotal = 0;

        lines = letterCountFile.split("\\n");
        for(String line : lines){
            parts =  line.split("\\s");
            int value = Integer.parseInt(parts[1]);
            letterFrequency.put(parts[0],value);
            letterTotal += value;
        }
        setScoreInHashmap(letterFrequency,letterTotal);
    }

    public void setScoreInHashmap(HashMap<String, Integer> inHashmap, int total){
        int totalScore = 0;
        for(String key : inHashmap.keySet()) {
            int score = (int) Math.ceil(inHashmap.get(key) * 1000 / total);
            if (score == 0) score = 1;
            totalScore += score;
            inHashmap.put(key, score);
//            System.out.println(key + ": " + score);
        }
//        System.out.println("Total: " + totalScore);
    }

    public int analayseWord(String word) {
        int score = 0;
        int combinationScore = 0;
        int letterFrequencyScore = 0;
        int vowelScore = 0;
        String vowels = "aeiou";

        String purgedString = word.replaceAll("[^A-Za-z]+", "");
        word = purgedString.toLowerCase();

        if (word.length() > 0){
            int startWithScore = startsWith.get(word.charAt(0) + "");
            int endWithScore = endsWith.get(word.charAt(word.length() - 1) + "");

            char[] charArray = word.toCharArray();
            for (int i = 0; i < charArray.length; i++) {
                char c = charArray[i];
                letterFrequencyScore += letterFrequency.get(c + "");
                if (vowels.contains(c + "")) vowelScore++;
                if (i < (charArray.length - 1)) {
                    String key = c + "" + word.charAt(i + 1);
                    Integer value = combinations.get(key);
                    combinationScore += (value == null) ? 0 : value;
                }
            }

            combinationScore /= (word.length() > 1)?(word.length() - 1):1;
            letterFrequencyScore /= word.length();
            vowelScore = vowelScore * 100 / word.length();

            score += startWithScore;
            score += endWithScore;
            score += combinationScore;
            score += letterFrequencyScore;
            score += vowelScore;
        }

        return score;
    }

    public int analayseSentence(String sentence){
        int averageScore = 0;
        String[] words = sentence.split("\\s");
        for(String word : words){
            averageScore += analayseWord(word);
        }
        averageScore /= words.length;
        return averageScore;
    }
}
