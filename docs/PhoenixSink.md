# Phoenix Source

Ensure that you are using a version of database-plugins later than 1.8.4. These have an improvement over version 1.8.4, in that they contain a Database sink plugin that properly supports the phoenix jdbc driver.


Save the following JSON to a file as phoenix-client.json:

    {
      "parents" : [ "system:cdap-etl-batch[3.2.0,10.0.0)", "system:cdap-data-pipeline[3.4.0,10.0.0)" ],
      "plugins" : [
        {
          "name": "phoenix-client",
          "type": "jdbc",
          "description": "Apache Phoenix JDBC external plugin",
          "className": "org.apache.phoenix.jdbc.PhoenixDriver"
        }
      ]
    }




Note that the phoenix jdbc driver is located on the Phoenix cluster in a path such as /usr/hdp/current/phoenix-client/phoenix-client.jar

You can upload this jdbc driver to CDAP using the CDAP CLI command:
load artifact <path to jdbc driver> config-file <path to phoenix-client.json>

For example:

    load artifact /usr/hdp/current/phoenix-client/phoenix-client.jar config-file /tmp/phoenix-client.json



To write to a Phoenix table, use a Database sink:
- Specify the table name
- As the plugin name, specify “phoenix-client”
- Specify the connection string. For example: jdbc:phoenix:<HOSTNAME>:2181:/hbase-unsecure;
- For Enable Upset, set “true”
- For Enable Transaction Isolation, set “false”
- Specify the list of columns being written

