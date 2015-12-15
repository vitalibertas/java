package com.company.hosting.hive;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.DatabaseReader.Builder;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Subdivision;
import com.maxmind.geoip2.record.Location;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;

@UDFType(deterministic = true)
@Description(
        name="IpToGeoMap",
        value="Returns the geographical information of an IP address (string).",
        extended="ADD JAR /home/<user>/maxmind/hive_udf-1.0.jar; CREATE TEMPORARY FUNCTION ip2geo AS 'com.company.hosting.hive.IpToGeoMap'; SELECT ip2geo('country','192.168.1.1');"
)
public class IpToGeo extends GenericUDF {

    StringObjectInspector oiTrait;
    StringObjectInspector oiIp;
    StringObjectInspector oiPath;
    private String hadoopPath = "/<user>/maxmind/";
    private static final String MAXMIND_DB = "GeoIP2-City.mmdb";
    private static DatabaseReader reader;

    @Override
    public String getDisplayString(String[] children) {
            assert (children.length > 0);
            return "IpToGeoMap(" + children[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
                throw new UDFArgumentLengthException("IpToGeoMap() accepts these arguments: Geo trait, IP address, [and optionally database path]. " + arguments.length + " found.");
        }

        for (int a = 0; a < arguments.length; a++) {
            checkStringObject(arguments[a]);
        }

        this.oiTrait = (StringObjectInspector) arguments[0];
        this.oiIp = (StringObjectInspector) arguments[1];

        if (arguments.length == 3) {
            this.oiPath = (StringObjectInspector) arguments[2];
        }

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {

        String geoTrait = this.oiTrait.getPrimitiveJavaObject(arguments[0].get());
        String ip = this.oiIp.getPrimitiveJavaObject(arguments[1].get());
        if (arguments.length == 3) {
            hadoopPath = this.oiPath.getPrimitiveJavaObject(arguments[2].get());
        }

        InetAddress ipAddress;
        String geoResult = "";

        try {
            ipAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            return new Text("");
        }

        try {
            if (reader == null) {
                Path databasePath = new Path(hadoopPath + MAXMIND_DB);
                reader = getReader(databasePath);
            }

            // GeoTrait can be country, subdivision, city, or location.
            switch (geoTrait) {
                case "country":
                    geoResult = getCountry(reader.city(ipAddress));
                    break;
                case "subdivision":
                    geoResult = getSubdivision(reader.city(ipAddress));
                    break;
                case "city":
                    geoResult = getCity(reader.city(ipAddress));
                    break;
                case "location":
                    geoResult = getLocation(reader.city(ipAddress));
                    break;
                default:
                    throw new UDFArgumentException(geoTrait + " is an unsupported lookup. The lookup trait can be country, subdivision, city, or location.");
            }
            return new Text(geoResult);
        } catch (UDFArgumentException e) {
            throw new UDFArgumentException(e.getMessage());
        } catch (AddressNotFoundException e) {
            return new Text("");
        } catch (Exception e) {
            throw new UnsupportedOperationException("Problem with retrieving information for [ " + ip + " ] !!", e);
        }
    }

    private static synchronized DatabaseReader getReader(Path databasePath) throws IOException {
        // if you add 'synchronized' to getReader(), and do the 'if not null use it / if null create it',
        // the 'synchronized' acts as a mutex and makes sure only one DatabaseReader is created at a time.
        
        // A HDFS Filesystem object pointing to your GeoIP2 or GeoLite2 database
        final FileSystem fs = FileSystem.get(databasePath.toUri(), new Configuration());

        // This creates the DatabaseReader object, which should be reused across
        // lookups.
        final DatabaseReader reader = new DatabaseReader.Builder(fs.open(databasePath)).build();

        if (reader != null) {
            return reader;
        } else {
            throw new IOException("Problem reading Maxmind database at " + databasePath + MAXMIND_DB);
        }
    }

    private static void checkStringObject(ObjectInspector argument) throws UDFArgumentException {
        if (!(argument instanceof StringObjectInspector)) {
            throw new UDFArgumentException("The arguments must be strings.");
        }
    }

    public static String removeNull(String name) {
         return (name == null ? "" : name);
     }

    public static String getCountry(CityResponse response) {
        Country country = response.getCountry();

        return removeNull(country.getName());
     }

    public static String getSubdivision(CityResponse response) {
        Subdivision subdivision = response.getMostSpecificSubdivision();

        return removeNull(subdivision.getName());
     }

    public static String getCity(CityResponse response) {
        City city = response.getCity();

        return removeNull(city.getName());
     }

    public static String getLocation(CityResponse response) {
        Location location = response.getLocation();
        String latitude = removeNull(location.getLatitude().toString());
        String longitude = removeNull(location.getLongitude().toString());

        return (latitude + "," + longitude);
     }
}
