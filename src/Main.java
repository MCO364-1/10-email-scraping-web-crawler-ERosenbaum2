import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Main {
    private static List<String> urlsToVisit = Collections.synchronizedList(new LinkedList<>());
    private static Set<String> scrapedEmailAddresses = Collections.synchronizedSet(new HashSet<>());
    private static Set<String> scrapedUrls = Collections.synchronizedSet(new HashSet<>());
    private static List<Email> emailList = Collections.synchronizedList(new LinkedList<>());
    private static AtomicInteger numEmailsScraped = new AtomicInteger(0);
    private static int emailTarget = 10000;
    private static int numThreads = 16;
    private static String url = "https://touro.edu/";
    private static String connectionUrl = "jdbc:sqlserver://database-1.ckxf3a0k0vuw.us-east-1.rds.amazonaws.com;"
            + "database=XXXXXXXXX;"//Removed sensitive information
            + "user=XXXXXXXXX;"
            + "password=XXXXXXXXX;"
            + "encrypt=true;"
            + "trustServerCertificate=true;"
            + "loginTimeout=30;";

    public static void main(String[] args) {
        urlsToVisit.add(url);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executor.submit(new MyTask());
        }
        while (numEmailsScraped.get() < emailTarget) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        executor.shutdown();
        //Sending all emails to the database
        String insertSQL = "INSERT INTO Emails (emailAddress, source, timeStamp) VALUES (?, ?, ?)";
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            for (Email email : emailList) {
                preparedStatement.setString(1, email.getEmailAddress());
                preparedStatement.setString(2, email.getSource());
                preparedStatement.setTimestamp(3, new Timestamp(email.getTimeStamp()));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Email {
        private String emailAddress;
        private String source;
        private long timeStamp;

        public Email(String emailAddress, String source, long timeStamp) {
            this.emailAddress = emailAddress;
            this.source = source;
            this.timeStamp = timeStamp;
        }

        public String getEmailAddress() {
            return emailAddress;
        }

        public String getSource() {
            return source;
        }

        public long getTimeStamp() {
            return timeStamp;
        }
    }

    private static class MyTask implements Runnable {
        @Override
        public void run() {//Each thread will scrape the next url in the queue, retrieving the urls and the emails
            while (numEmailsScraped.get() < emailTarget) {
                try {
                    String currentUrl = null;
                    synchronized (urlsToVisit) {
                        if (!urlsToVisit.isEmpty()) {//Get the next url from the queue if not empty
                            currentUrl = urlsToVisit.remove(0);
                        }
                    }
                    if (scrapedUrls.add(currentUrl)) {//Only scrape the page if it hasn't been scraped yet
                        try {
                            Document doc = Jsoup.connect(currentUrl).get();
                            System.out.println("Scraped page " + currentUrl);
                            //Getting the emails from the scraped page
                            Pattern emailRegex = Pattern.compile("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,6}");
                            Matcher matcher = emailRegex.matcher(doc.text());
                            while (matcher.find() && numEmailsScraped.get() < emailTarget) {
                                String email = matcher.group();
                                if (scrapedEmailAddresses.add(email)) {//Only add the email if it is unique
                                    int count = numEmailsScraped.incrementAndGet();
                                    System.out.println("Found email #" + count + ": " + email + " from " + currentUrl);
                                    emailList.add(new Email(email, currentUrl, System.currentTimeMillis()));//Store email with its source and timestamp for later batch save
                                }
                            }
                            //Getting the links from the scraped page and adding it to the end of the queue
                            Elements links = doc.select("a[href]");
                            List<String> linksOnPage = new ArrayList<>();
                            for (Element link : links) {
                                String href = link.attr("abs:href");
                                if (href.matches("^https://.*(?<!\\.)$") && !scrapedUrls.contains(href)) {//only add links that are not already scraped
                                    linksOnPage.add(href);
                                }
                            }
                            if (!linksOnPage.isEmpty()) {
                                synchronized (urlsToVisit) {
                                    urlsToVisit.addAll(linksOnPage);//Add all links to the end of the queue
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}