package ri;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;



public class Check extends UDF {

    private HTable htable;

    public Check() {
        try {
            Configuration conf = HBaseConfiguration.create();
            htable = new HTable(conf, "bfd_keys");
        } catch (IOException ioe) {
            throw new IllegalStateException("Unable to open HTable.", ioe);
        }
    }

    public Text evaluate(Text key) {
        StringBuilder sb = new StringBuilder (key.toString());

        // While the string we get is null terminated, drop the last character.
        while(sb.charAt(sb.length() - 1) == '\0') {
            sb.setLength(sb.length() - 1);
        }

        Get get = new Get(sb.toString().getBytes());
        try {
            Result result = htable.get(get);
            if(result.isEmpty()) {
                return new Text("KO");
            } else {
                return new Text("OK");
            }
        } catch (IOException ioe) {
            return new Text("UH-OH");
        }

    }
}
