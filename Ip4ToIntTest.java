package com.company.hosting.hive;

import junit.framework.TestCase;
import junit.framework.Assert;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;


public class IpTest
    extends TestCase
{
    public void testBadIPv4() {
        Text ipAddress = new Text("312-414-5597");
        Ip4ToBigInt example = new Ip4ToBigInt();

        Assert.assertEquals(new LongWritable(-1), example.evaluate(ipAddress));
    }

    public void testNullIPv4() {
        Text ipAddress = null;
        Ip4ToBigInt example = new Ip4ToBigInt();

        Assert.assertEquals(new LongWritable(-1), example.evaluate(ipAddress));
    }

    public void testGoodIPv4() {
        Text ipAddress = new Text("192.168.1.10");
        Ip4ToBigInt example = new Ip4ToBigInt();

        Assert.assertEquals(new LongWritable(3232235786L), example.evaluate(ipAddress));
    }

}
