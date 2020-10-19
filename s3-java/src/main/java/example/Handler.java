package example;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;


public class Handler implements RequestHandler<S3Event, String> {
    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    @Override
    public String handleRequest(S3Event s3event, Context context) {
        try {
            S3EventNotificationRecord record = s3event.getRecords().get(0);

            String srcBucket = record.getS3().getBucket().getName();
            String srcKey = record.getS3().getObject().getUrlDecodedKey();

            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                    srcBucket, srcKey));
            InputStream objectData = s3Object.getObjectContent();

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            List<Book> books = null;
            try {
                DocumentBuilder domBuilder = factory.newDocumentBuilder();
                Document doc = domBuilder.parse(objectData);
                if (doc.getDocumentElement().getNodeName().equals("books")) {
                    books = parseBookList(doc);
                }
            } catch (ParserConfigurationException e) {
                throw new RuntimeException(e);
            } catch (SAXException e) {
                e.printStackTrace();
            }
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
            if (books != null && books.size() > 0) {
                String dynamodbTable = "Books";
                logger.info("Writing to table: " + dynamodbTable);
                try {
                    DynamoDBMapper mapper = new DynamoDBMapper(client);
                    books.stream()
                            .filter(book -> !StringUtils.isNullOrEmpty(book.getAuthor()) && !StringUtils.isNullOrEmpty(book.getTitle()))
                            .forEach(mapper::save);
                } catch (AmazonDynamoDBException e) {
                    logger.error(e.getErrorMessage());
                    throw new RuntimeException(e);
                }
                logger.info("Successfully parsed " + srcBucket + "/"
                        + srcKey + " and uploaded books to dynamodb table:" + dynamodbTable);
                return "Ok";
            }
            logger.info("Successfully parsed " + srcBucket + "/"
                    + srcKey + " and no books were detected");
            return "Ok";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Book> parseBookList(Document doc) {
        NodeList bookList = doc.getElementsByTagName("book");
        List<Book> books = new ArrayList<>();
        for (int i = 0; i < bookList.getLength(); i++) {
            Node curNode = bookList.item(i);
            if (curNode.getNodeType() == Node.ELEMENT_NODE) {
                Element nodeElement = (Element) curNode;
                Book book = new Book();
                book.setAuthor(nodeElement.getElementsByTagName("author").item(0).getTextContent());
                book.setTitle(nodeElement.getElementsByTagName("title").item(0).getTextContent());
                book.setGenre(nodeElement.getElementsByTagName("genre").item(0).getTextContent());
                book.setYear(nodeElement.getElementsByTagName("year").item(0).getTextContent());
                books.add(book);
            }
        }
        return books;
    }
}