package ri;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Key extends UDF {

    public Text evaluate(Text table, Text a) {
        StringBuilder sb = new StringBuilder();
        sb.append(table.toString());
        sb.append(':');
        sb.append(a.toString());
        String salt = String.format("%04d", (sb.toString().hashCode() & Integer.MAX_VALUE) % 1000);
        sb.insert(0, salt + ":");
        return new Text(sb.toString());
    }

    public Text evaluate(Text table, Text a, Text b) {
        StringBuilder sb = new StringBuilder();
        sb.append(table.toString());
        sb.append(':');
        sb.append(a.toString());
        sb.append(':');
        sb.append(b.toString());
        String salt = String.format("%04d", (sb.toString().hashCode() & Integer.MAX_VALUE) % 1000);
        sb.insert(0, salt + ":");
        return new Text(sb.toString());
    }
    public Text evaluate(Text table, Text a, Text b, Text c) {
        StringBuilder sb = new StringBuilder();
        sb.append(table.toString());
        sb.append(':');
        sb.append(a.toString());
        sb.append(':');
        sb.append(b.toString());
        sb.append(':');
        sb.append(c.toString());
        String salt = String.format("%04d", (sb.toString().hashCode() & Integer.MAX_VALUE) % 1000);
        sb.insert(0, salt + ":");
        return new Text(sb.toString());
    }
    public Text evaluate(Text table, Text a, Text b, Text c, Text d) {
        StringBuilder sb = new StringBuilder();
        sb.append(table.toString());
        sb.append(':');
        sb.append(a.toString());
        sb.append(':');
        sb.append(b.toString());
        sb.append(':');
        sb.append(c.toString());
        sb.append(':');
        sb.append(d.toString());
        String salt = String.format("%04d", (sb.toString().hashCode() & Integer.MAX_VALUE) % 1000);
        sb.insert(0, salt + ":");
        return new Text(sb.toString());
    }
    public Text evaluate(Text table, Text a, Text b, Text c, Text d, Text e) {
        StringBuilder sb = new StringBuilder();
        sb.append(table.toString());
        sb.append(':');
        sb.append(a.toString());
        sb.append(':');
        sb.append(b.toString());
        sb.append(':');
        sb.append(c.toString());
        sb.append(':');
        sb.append(d.toString());
        sb.append(':');
        sb.append(e.toString());
        String salt = String.format("%04d", (sb.toString().hashCode() & Integer.MAX_VALUE) % 1000);
        sb.insert(0, salt + ":");
        return new Text(sb.toString());
    }

    public Text evaluate(Text table, Text a, Text b, Text c, Text d, Text e, Text f) {
        StringBuilder sb = new StringBuilder();
        sb.append(table.toString());
        sb.append(':');
        sb.append(a.toString());
        sb.append(':');
        sb.append(b.toString());
        sb.append(':');
        sb.append(c.toString());
        sb.append(':');
        sb.append(d.toString());
        sb.append(':');
        sb.append(e.toString());
        sb.append(':');
        sb.append(f.toString());
        String salt = String.format("%04d", (sb.toString().hashCode() & Integer.MAX_VALUE) % 1000);
        sb.insert(0, salt + ":");
        return new Text(sb.toString());
    }

}
