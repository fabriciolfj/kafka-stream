package guru.learningjournal.kafka.examples;

import java.math.BigDecimal;

public class teste {

    public static void main(String[] args) {
        BigDecimal t = new BigDecimal("80.0");
        t.setScale(2);
        System.out.println(t);

        BigDecimal t2 = new BigDecimal(t.toString()).setScale(2);
        System.out.println(t2);
    }
}
