
package com.company.hosting.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;


@Description(
        name="Ip4ToBigInt",
        value="Returns the integer conversion of an IPv4 address (string).",
        extended="SELECT Ip4ToBigInt('192.168.1.1');"
)
public class Ip4ToBigInt extends UDF {

    public static LongWritable evaluate( Text ipAddress ) {
        if (ipAddress != null) {

            String[] ip4array = ipAddress.toString().split("\\.");

            long bigIntIp = 0;

            if (ip4array.length == 4) {
                for (int i=0; i < 4; i++) {
                    int power = 3 - i;
                    int octet = Integer.parseInt(ip4array[i]);
                    if (octet < 256 && octet >= 0) {
                        bigIntIp += (octet * Math.pow(256, power));
                    }
                    else return new LongWritable(-1);
                }
                return new LongWritable(bigIntIp);
            }
        }
        return new LongWritable(-1);

    }
}
