package hadoopReviewsAdditionals;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Main {

	static private String regEx;


    // merge_files
    // parameter:  path= the path of the files (pos/neg)
    // merges 10 reviews in one text
    // everyline starts with the review name
    // eg. pos/cv_1000_1000 this text is found in the cv_1000_1000 file
    public static void merge_files(String path) throws FileNotFoundException, IOException {

        FileInputStream fis;
        FileOutputStream fos;
        DataInputStream dis;
        BufferedOutputStream bos;
        File file = new File(path);
        File[] file_names;
        file_names = new File[file.listFiles().length];
        file_names = file.listFiles();

        try {
            new File("reviews").mkdir();
        } catch (Exception e) {
            System.out.println(e);

        }
        for (int i = 0; i < file_names.length;) {
            FileWriter fstream2;
            if ((i + 11) <= file_names.length) {
                fstream2 = new FileWriter("reviews/" + path + (i + 1) + "-" + (i + 11) + ".txt");
            } else {
                fstream2 = new FileWriter("reviews/" + path + (i + 1) + "-" + (file_names.length) + ".txt");
            }
            BufferedWriter out = new BufferedWriter(fstream2);
            for (int j = 0; j < 10; j++) {

                FileInputStream fstream = new FileInputStream(file_names[i + j]);
                DataInputStream in = new DataInputStream(fstream);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

                String strLine;
                out.write(file_names[i + j].toString());
                out.newLine();
                out.newLine();
                while ((strLine = br.readLine()) != null) {         

                    if (strLine.trim().length() == 0) {             
                        continue;
                    }
                    out.write(file_names[i + j] + " " + strLine);     // apothikefsi twn reviews xwris ta arthra
                    out.newLine();
                }
                in.close();
                out.newLine();
                out.newLine();
            }
            out.close();
            i += 10;
        }
    }


    




    // returns neg for originally negative reviews
    //         pos for originally positive reviews
    // used by compare results
    public static String getNegOrPos(String text) {
        Pattern pattern = Pattern.compile("pos");
        if (pattern.matcher(text).find()) {
            return "pos";
        }
        return "neg";
    }

    // returns the original name of the review
    // used by compare results
    public static String getReviewName(String text) {
        Pattern pattern = Pattern.compile("cv[0-9]+_[0-9]+\\.txt");
        Matcher m = pattern.matcher(text);
        m.find();
        String result = m.group();
        return result;
    }


    // compares the results
    public static void compareResults(String file_path) throws IOException {

        try {
            FileInputStream fstream = new FileInputStream(file_path);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int counterpos = 0;
            int counterneg = 0;
            int counterPosPos = 0;
            int counterPosNeg = 0;
            int counterNegNeg = 0;
            int counterNegPos = 0;
            ArrayList<String> posNeg = new ArrayList<String>();
            ArrayList<String> negPos = new ArrayList<String>();

            while ((strLine = br.readLine()) != null) {             
                if (strLine.trim().length() == 0) {                 
                    continue;
                }
                String[] strArr = strLine.split("\t");
                String description = getNegOrPos(strArr[0]);
                String name = getReviewName(strArr[0]);
                if (description.equals("pos")) {
                    counterpos++;
                    if (Float.parseFloat(strArr[1]) > 0.0) {
                        counterPosPos++;
                    } else if (Float.parseFloat(strArr[1]) < 0.0) {
                        counterPosNeg++;
                        posNeg.add(name);
                    }
                } else if (description.equals("neg")) {
                    counterneg++;
                    if (Float.parseFloat(strArr[1]) < 0.0) {
                        counterNegNeg++;
                    } else if (Float.parseFloat(strArr[1]) > 0.0) {
                        counterNegPos++;
                        negPos.add(name);
                    }

                }



            }
			
			float perPP = 100*counterPosPos/counterpos;
			float perPosNotPos = 100-perPP;
			float perNN = 100*counterNegNeg/counterneg;
			float perNegNotNeg = 100-perNN;
			float accuracy = 100*(counterPosPos+counterNegNeg)/(counterpos+counterneg);


            System.out.println("Positive reviews                  : " + counterpos);
            System.out.println("Positive reviews -> Positive rated: " + counterPosPos);
            System.out.println("Positive reviews -> Negative rated: " + counterPosNeg);
            System.out.println("Positive reviews -> 0.0 rated     : " + (counterpos - counterPosPos - counterPosNeg));
			System.out.println("Positive accuracy                 : " + perPP + " %");
            System.out.println("");
            System.out.println("Negative reviews                  : " + counterneg);
            System.out.println("Negative reviews -> Negative rated: " + counterNegNeg);
            System.out.println("Negative reviews -> Positive rated: " + counterNegPos);
            System.out.println("Negative reviews -> 0.0 rated     : " + (counterneg - counterNegNeg - counterNegPos));
			System.out.println("Negative accuracy                 : " + perNN + " %");

            System.out.println("");
            System.out.println("The Positive -> Negative rated reviews");
            for (int i = 0; i < posNeg.size(); i++) {
                System.out.print(posNeg.get(i) + " ");
                if (i % 50 == 0) {
                    System.out.println("");
                }
            }

            System.out.println("");
            System.out.println("");
            System.out.println("The Negative -> Positive rated reviews");
            for (int i = 0; i < negPos.size(); i++) {
                System.out.print(negPos.get(i) + " ");
                if (i % 50 == 0) {
                    System.out.println("");
                }
            }
			System.out.println("\n");
		    System.out.println("Overall accuracy : "+ accuracy +" %"); 

          } catch (Exception e) {
            System.out.println(e);
        }

    }



    // returns an ArrayList of the words shown more than 150 times
    public static ArrayList<String> findMostFrequentWord(String path) {
        ArrayList<String> words = new ArrayList<String>();
        try {
            FileInputStream fstream = new FileInputStream(path);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;

            while ((strLine = br.readLine()) != null) {             
                if (strLine.trim().length() == 0)                   
                {
                    continue;
                }
                String[] strArr = strLine.split("\t");
                if (Integer.parseInt(strArr[1]) > 500) {
                    words.add(strArr[0]);
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }

	// for(String s:words)
	//     System.out.println(s);
        return(words);

    }


    // filters the text                                   is used at mapper
    public static String reg(String text) {
        Pattern pattern = Pattern.compile(regEx);       //  \\b word boundaries
        return pattern.matcher(text).replaceAll("");
    }

	// adds the most frequently shown words at the regular expression
    // creates the regular expression that we will use to filter the lines at the mapper
    public static void editRegEx(ArrayList<String> words){
        if(words.size()>0){
            regEx=regEx+"|(?:\\b(?:";
        for(int i=12;i<words.size();i++){
            if(!words.get(i).contains("'")){
               regEx=regEx+words.get(i);
               if(i<(words.size()-1))
                   regEx=regEx+"|";
            }
        }
        regEx=regEx+")\\b)";
       }
        //System.out.println(regEx);
    }


    public static void main(String[] args) throws FileNotFoundException, IOException {
        
//        ArrayList<String> words=findMostFrequentWord("wcoutput");
//        regEx="(?:\\b(?:there|their|they|the|a|about|by|for|to|is|are|then|when|where|who|will|was|what|[0-9]+)\\b)|(?:!|\"|-|--|\\?|\\.|:|;)";
//        editRegEx(words);
//        System.out.println(regEx);

//        merge_files("pos");
//        merge_files("neg");
//        compareResults("output");

    }
}
