package at.jku.dke.dwh.enronassignment.objects;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

public class Email implements Serializable {
    private final String id;
    private final Timestamp date;
    private final String from;
    private final List<String> recipients;
    private final String subject;
    private final String body;

    public Email(String id, Timestamp date, String from, List<String> recipients, String subject, String body) {
        super();
        this.id = id;
        this.date = date;
        this.from = from;
        this.recipients = recipients;
        this.subject = subject;
        this.body = body;
    }

    public String getId() {
        return id;
    }

    public String getFrom() {
        return from;
    }

    public List<String> getRecipients() {
        return recipients;
    }

    public String getSubject() {
        return subject;
    }

    public String getBody() {
        return body;
    }

    public Timestamp getDate() {
        return date;
    }

    @Override
    public String toString() {
        return "Email{" +
                "id=" + id + '\n' +
                ", date=" + date.toString() + '\n' +
                ", from=" + from + '\n' +
                ", recipients(" + recipients.size() + ")=" + recipients + '\n' +
                ", subject=" + subject + '\n' +
                ", body=" + body + '\n' +
                "}";
    }
}
