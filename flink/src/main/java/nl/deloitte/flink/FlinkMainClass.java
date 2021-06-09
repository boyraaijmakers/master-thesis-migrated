package nl.deloitte.flink;

import edu.stanford.nlp.simple.Sentence;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import uk.ac.wlv.sentistrength.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

class FlinkMainClass {
    private static String pathToSWN = "SentiWordNet3.txt";

    private static HashMap<String, Double> _swn3Dict;
    private static NameFinderME nameFinder;
    private static SentiStrength sentiStrength;
    private static HashMap<String, Double> _senticNetDict;
    private static HashMap<String, Double> _densifyLexicon;

    public static void main(String[] args) {

        initSWN();
        // initOpenNLPFinder();
        // initSentiStrength();
        // initSenticNet();
        // initDensifyLex();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("NEWGS2-nh.csv").setParallelism(1);

        SingleOutputStreamOperator dataSA = mapSentiment(source);
        printSentimentPN(dataSA);

        SingleOutputStreamOperator dataNER = mapCompany(source);
        printNERResults(dataNER);

        try {
            env.execute("ArticleAnalytics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static SingleOutputStreamOperator mapSentiment(DataStreamSource source) {
        return source.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public  Tuple3<String, String, Integer> map(String s) throws Exception {
                try {
                    return new Tuple3<>(
                        s.split(";")[0].trim(), 
                        preProcess(s.split(";")[4]), 
                        Integer.parseInt(s.split(";")[8])
                    );
                } catch (Exception e) {
                    return new Tuple3<>(
                        s.split(";")[0].trim(), 
                        preProcess(s.split(";")[4]), 
                        1
                    );
                }
            }
        });
    }

    private static SingleOutputStreamOperator mapCompany(DataStreamSource source) {
        return source.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public  Tuple3<String, String, String> map(String s) throws Exception {
                return new Tuple3<>(
                    preProcess(s.split(";")[4]), 
                    s.split(";")[5].toLowerCase(), 
                    s.split(";")[0]
                );
            }
        });
    }

    private static void printSentimentPN(SingleOutputStreamOperator stream) {
        stream.map(new MapFunction<Tuple3<String, String, Integer>, Tuple3<String, Integer, String>>() {
            @Override
            public  Tuple3<String, Integer, String> map(Tuple3<String, String, Integer> in) throws Exception {
                double classifiedSentiment = classifySWN(in.f1);
                String sentimentRes = in.f2 == -1 ? (classifiedSentiment == -1 ? "TN" : "FP" ) : (classifiedSentiment == -1 ? "FN" : "TP");

                return new Tuple3<>(in.f0, in.f2, sentimentRes);
            }
        }).writeAsText("sentimentOut.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private static void printNERResults(SingleOutputStreamOperator stream) {
        stream.map(new MapFunction<Tuple3<String, String, String>, Tuple4<String, String, String, String>>() {
            @Override
            public  Tuple4<String, String, String, String> map(Tuple3<String, String, String> in) throws Exception {
                String stanford = getStanfordCompanies(in.f0, in.f1);
                String nel = dbpediaSpotlight(stanford.split(",")[0]);

                return new Tuple4<>(in.f2, in.f1, stanford, nel);
            }
        }).writeAsText("nerOut.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    // Pre-processing
    private static String preProcess(String corpus) {
        Set<String> stopWordSet = new HashSet<String>(Arrays.asList(stopwords));
        String clean = "";

        // Tokenize words and remove special characters
        for (String word : corpus.split(" ")) {
            clean += word.replaceAll("([^a-zA-Z\\s])", "") + " ";
        }

        return clean.trim().replaceAll(" +", " ");
    }

    // NER and NEL
    private static String getStanfordCompanies(String corpus, String label) {
        HashMap<String, Integer> results = new HashMap<>();
        Sentence sentence = new Sentence(corpus);
        List<String> nerTags = sentence.nerTags();

        boolean firstDone = false;
        String firstOrg = "";

        int i = 0;
        while ( i < nerTags.size()) {
            if (nerTags.contains("ORGANIZATION")) {
                String org = sentence.word(i);
                int j = 1;

                while (i + j < nerTags.size() && nerTags.get(i + j).equals("ORGANIZATION")) {
                    org += " " + sentence.word(i + j);
                    j++;
                }

                if (!firstDone) {
                    firstOrg = org;
                    firstDone = true;
                }

                if (results.containsKey(org)) {
                    results.put(org, results.get(org) + 1);
                } else {
                    results.put(org, 1);
                }

                i += j + 1;
            } else {
                i++;
            }
        }

        String company = "";
        int max = 0;
        int second = 0;

        boolean correct = false;
        boolean present = false;
        boolean aboveThreshold = false;
        boolean first = false;

        if (firstOrg.toLowerCase().contains(label)) {
            first = "1";
        }

        for (Map.Entry<String, Integer> row :  results.entrySet()){
            if (row.getValue() >= 2 && row.getKey().toLowerCase().contains(label)) aboveThreshold = "1";
        }

        for (String comp : results.keySet()){
            if (comp.toLowerCase().contains(label)) present = "1";
            if (results.get(comp) > max) {
                company = comp;
                second = max;
                max = results.get(comp);
            }
        }
        Array<Integer> dominance = new ArrayList<>();

        for(int i = 1; i <= 8; i++) {
            dominance.add(max - second >= i ? 1 : 0);
        }

        if (company.toLowerCase().contains(label)) correct = "1";

        return String.format("%s,%o,%o,%o,%o,%s", 
                            company,
                            present,
                            correct,
                            aboveThreshold,
                            first,
                            dominance.toString()); 
    }

    private static void initOpenNLPFinder() {
        try {
            InputStream inputStreamNameFinder = new FileInputStream("en-ner-organization.bin");
            TokenNameFinderModel model = new TokenNameFinderModel(inputStreamNameFinder);
            nameFinder = new NameFinderME(model);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getOpenNLPCompanies(String corpus, String label) {
        try {
            String[] sentence = corpus.split(" ");
            Span nameSpans[] = nameFinder.find(sentence);

            HashMap<String, Integer> res = new HashMap<>();

            for(Span span : nameSpans) {
                String word = sentence[span.getStart()].trim();
                if(word == "") continue;
                if (res.containsKey(word)) {
                    res.put(word, res.get(word) + 1);
                } else {
                    res.put(word, 1);
                }
            }

            String company = "";
            int max = 0;

            String present = "0";
            String correct = "0";

            for (String comp : res.keySet()){
                if (comp.toLowerCase().contains(label)) present = "1";
                if (res.get(comp) > max) {
                    company = comp;
                    max = res.get(comp);
                }
            }

            if (company.toLowerCase().equals(label)) correct = "1";

            company += "," + present + "," + correct;

            return company;

        } catch (Exception e) {
            return ",,";
        }
    }

    private static String dbpediaSpotlight(String corpus) {
        try {
            StringBuilder result = new StringBuilder();
            URL url = new URL("http://localhost:2222/rest/annotate?text=" + 
                                URLEncoder.encode(corpus, "UTF-8"));
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            rd.close();

            String linkedUrl = result.toString().split("\"")[9];

            return result.toString();
        } catch (Exception e) {
            return "";
        }
    }

    // Sentiment Analysis
    private static void initSWN(){
        _swn3Dict = new HashMap<String, Double>();
        HashMap<String, Vector<Double>> _temp = new HashMap<>();
        try {
            BufferedReader csv = new BufferedReader(new FileReader(pathToSWN));
            String line = "";
            while ((line = csv.readLine()) != null) {
                if (line.startsWith("#")) continue;
                String[] data = line.split("\t");
                Double score = Double.parseDouble(data[2]) - Double.parseDouble(data[3]);
                String[] words = data[4].split(" ");
                for (String w : words) {
                    String[] w_n = w.split("#");
                    w_n[0] += "#" + data[0];
                    int index = Integer.parseInt(w_n[1]) - 1;
                    if (_temp.containsKey(w_n[0])) {
                        Vector<Double> v = _temp.get(w_n[0]);
                        if (index > v.size())
                            for (int i = v.size(); i < index; i++)
                                v.add(0.0);
                        v.add(index, score);
                        _temp.put(w_n[0], v);
                    } else {
                        Vector<Double> v = new Vector<Double>();
                        for (int i = 0; i < index; i++)
                            v.add(0.0);
                        v.add(index, score);
                        _temp.put(w_n[0], v);
                    }
                }
            }
            Set<String> temp = _temp.keySet();
            for (Iterator<String> iterator = temp.iterator(); iterator.hasNext(); ) {
                String word = iterator.next();
                Vector<Double> v = _temp.get(word);
                double score = 0.0;
                double sum = 0.0;
                for (int i = 0; i < v.size(); i++)
                    score += (1D / (double) (i + 1)) * v.get(i);
                for (int i = 1; i <= v.size(); i++)
                    sum += 1D / (double) i;
                score /= sum;
                _swn3Dict.put(word, score);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int classifySWN(String corpus) {
        String[] words = corpus.split(" ");
        double totalScore = 0;
        for (String word : words) {

            Double total = 0D;
            if (_swn3Dict.get(word + "#n") != null) total += _swn3Dict.get(word + "#n");
            if (_swn3Dict.get(word + "#a") != null) total += _swn3Dict.get(word + "#a");
            if (_swn3Dict.get(word + "#r") != null) total += _swn3Dict.get(word + "#r");
            if (_swn3Dict.get(word + "#v") != null) total += _swn3Dict.get(word + "#v");

            totalScore += total;
        }

        totalScore = Math.round(totalScore * 100) / 100D;

        return (totalScore >= 0.0) ? 1 : -1;
    }

    private static void initSentiStrength() {
        sentiStrength = new SentiStrength();
        String ssthInitialisation[] = {"sentidata", "SentStrength_Data_Sept2011", "binary"};
        sentiStrength.initialise(ssthInitialisation);
    }

    private static int classifySentiStrength(String corpus) {
        String[] res = sentiStrength.computeSentimentScores(corpus).split(" ");
        return res[2].equals("1") ? 1 : -1;
    }

    private static void initSenticNet() {
        _senticNetDict = new HashMap<String, Double>();
        try {
            BufferedReader csv = new BufferedReader(new FileReader("senticnet5.csv"));
            String line = csv.readLine();
            while ((line = csv.readLine()) != null) {
                String[] data = line.split(";");
                double score = Double.parseDouble(data[2]);
                _senticNetDict.put(data[0].replaceAll("_", " "), score);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int classifySenticNet(String corpus, int ngramMin, int ngramMax) {
        String[] words = corpus.split(" ");
        double totalScore = 0;
        for (int i = ngramMin; i <= ngramMax; i++){
            for (int j = 0; j < words.length - i; j++ ) {
                String word = words[j];
                for (int k = 1; k < i; k++ ) {
                    word += " " + words[j+k];
                }

                if (_senticNetDict.containsKey(word)) {
                    totalScore += _senticNetDict.get(word);
                }
            }
        }

        totalScore = Math.round(totalScore * 100) / 100D;

        return (totalScore >= 0.0D) ? 1 : -1;
    }

    private static void initDensifyLex() {
        _densifyLexicon = new HashMap<String, Double>();
        try {
            BufferedReader csv = new BufferedReader(new FileReader("densify_news.txt"));
            String line;
            while ((line = csv.readLine()) != null) {
                String[] data = line.split(" ");
                double score = Double.parseDouble(data[1]);
                _densifyLexicon.put(data[0].replaceAll("_", " "), score);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int classifyDensify(String corpus, int ngramMin, int ngramMax) {
        String[] words = corpus.split(" ");
        double totalScore = 0;
        for (int i = ngramMin; i <= ngramMax; i++){
            for (int j = 0; j < words.length - i; j++ ) {
                String word = words[j];
                for (int k = 1; k < i; k++ ) {
                    word += " " + words[j+k];
                }

                if (_densifyLexicon.containsKey(word)) {
                    totalScore += _densifyLexicon.get(word);
                }
            }
        }

        totalScore = Math.round(totalScore * 100) / 100D;

        return (totalScore >= 0.0D) ? 1 : -1;
    }

}



