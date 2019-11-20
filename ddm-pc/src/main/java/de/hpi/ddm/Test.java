package de.hpi.ddm;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by nicolashoeck on 19.11.19.
 */
public class Test {

    public static void main(String[] args) {
        String[] message = new String[14];
        message[0] = "1";
        message[1] = "Sophia";
        message[2] = "ABCDEFGHIJK";
        message[3] = "10";
        message[4] = "c4712866799881ac48ca55bf78a9540b1883ae033b52109169eb784969be09d5";
        message[5] = "1582824a01c4b842e207a51e3cfc47212885e58eb147e33ea29ba212e611904d";
        message[6] = "e91aca467f5a2a280213a46aa11842d92577322b5c9899c8e6ffb3dc4b1d80a1";
        message[7] = "52be0093f91b90872aa54533b8ee9b38f794999bae9371834eca23ce51139b99";
        message[8] = "8052d9420a20dfc6197d514263f7f0d67f1296569f3c0708fa9030b08a4a908a";
        message[9] = "ca70f765d8c1b9b7a2162b19ea8e2b410166840f67ee276d297d0ab3bc05f425";
        message[10] = "570d3ada41deeb943fde8f397076eaef39862f12b0496eadee5b090face72eb5";
        message[11] = "f224061bd0359a5ca697570df620c34ecda0454fde04f511e4c8608b1f19acb8";
        message[12] = "01d8adcfdb790125e585e7ed9b77741cd09596dd7c15dcfdc75382a31a4f74ad";
        message[13] = "4b47ac115f6a91120d444638be98a97d009b9c13fa820d66796d2dad30d18975";


            // TO-Do Permutationen mit hashes irgendwo zentral erzeugen (auf jeden fall nicht jedes mal neu hier?)
            // z.B. https://stackoverflow.com/questions/8717375/how-to-effectively-store-a-large-set-of-permutations
            Set<Character> passwordChars = new HashSet();
            for(char c : message[2].toCharArray()) {passwordChars.add(c);}
            int passwordLength = Integer.valueOf(message[3]);
            String passwordHash = message[4];
            int amountOfHints = message.length - 5;
            int foundHints = amountOfHints;
            String password = "password cracking did not work";

            // Generate all permutations for hints based on password chars (length -1)
            ArrayList<String> hintPermutations = new ArrayList();

            //In der folgenden Zeile gibt es einen GC overhead limit exceeded (outofmemoryerror)
            //--> Es muss mit den Permutationen irgendwie effizienter umgegangen werden
            //--> Andere Möglichkeit: Check direkt in heapPermutation Methode
            heapPermutation(message[2].toCharArray(),message[2].toCharArray().length,message[2].toCharArray().length-1, hintPermutations);
            System.out.println(hintPermutations);

            for (String permutation: hintPermutations) {
                if(foundHints == amountOfHints){break;}
                String hash = hash(permutation);
                // Check if any hashed permutation is a hint (5 to amount of hints)
                for(int i = 5; i < message.length; i++) {

                    // if hint is found, apply hint
                    if(hash.equalsIgnoreCase(message[i])){
                        foundHints ++;
                        for(char c : passwordChars){
                            // Remove char that does not appear in hint from passwordchars
                            if(permutation.indexOf(c) < 0){passwordChars.remove(c);}}
                    }
                }
            }

            // After applying all hints, generate permutation for remaining password chars and test password
            ArrayList<String> passwordPermutations = new ArrayList<>();
            heapPermutation(passwordChars.toString().toCharArray(),passwordChars.size(), passwordLength, passwordPermutations);
            // Generate hash for each passwordPermutation and check if it is our desired password
            for(String permutation: passwordPermutations){
                String hash = hash(permutation);
                if(passwordHash.equalsIgnoreCase(hash)){password = permutation; break;}
            }

            System.out.println(password);


    }
    private static String hash(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));

            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        }
        catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    private static void heapPermutation(char[] a, int size, int n, List<String> l) {
        // If size is 1, store the obtained permutation
        if (size == 1)
            l.add(new String(a));

        for (int i = 0; i < size; i++) {
            heapPermutation(a, size - 1, n, l);

            // If size is odd, swap first and last element
            if (size % 2 == 1) {
                char temp = a[0];
                a[0] = a[size - 1];
                a[size - 1] = temp;
            }

            // If size is even, swap i-th and last element
            else {
                char temp = a[i];
                a[i] = a[size - 1];
                a[size - 1] = temp;
            }
        }
    }



}
