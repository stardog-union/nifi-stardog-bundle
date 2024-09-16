# Stardog NiFi Plugin

This repository contains the Stardog processors for Apache NiFi system.

## Building

Run the following command to build the NiFi .nar file:

```
mvn package
```

Copy the .nar file to NiFi lib directory:

``` 
cp *-nar/target/*.nar $NIFI_DIR/lib/
```

Restart NiFi for changes to take effect.

## Testing

Unit tests can be run through Maven by running the `mvn test` command. By default, any tests that require a Stardog
instance will be ignored. To enable those tests, set the following environment variables:

| Environment Variable    | Required | Default Value |
|-------------------------|----------|---------------|
| `NIFI_STARDOG_ENDPOINT` | Yes      |               |
| `NIFI_STARDOG_USERNAME` | No       | `admin`       |
| `NIFI_STARDOG_PASSWORD` | No       | `admin`       |


For example, the following endpoint will use a database named `nifi` hosted on a locally running Stardog instance:
```
NIFI_STARDOG_ENDPOINT=http://localhost:5820/nifi
```

The selected database should have the following
[database properties](https://docs.stardog.com/operating-stardog/database-administration/database-configuration#reasoning) set:

| Option                    | Value                 |
|---------------------------|-----------------------|
| `reasoning.schema.graphs` | `urn:g1,urn:g2`       |
| `reasoning.schemas`       | `g1=urn:g1,g2=urn:g2` |

## Using the NiFi Connector
Start the NiFi server by running the command `bin/nifi.sh start` in the NiFi installation folder.
It takes up to a minute for the NiFi server to start. Once the server is running, you can go to the URL
[http://localhost:8080/nifi](http://localhost:8080/nifi) in your browser, which will show the empty workflow.
You can drag the processor icon from the top left to the empty canvas and add a Stardog processor:

![NiFi Demo](/assets/images/nifidemo.gif)

Once the processor is added, you can change the parameters to specify the Stardog server to connect to, credentials, etc. See the following example for more details:

### Example NiFi Workflow

An example NiFi workflow is provided in this repository [here](/assets/example).
The workflow is for loading the Covid19 dataset published by the New York Times on GitHub into Stardog. It contains three processors:

1. NiFi's built-in processor to retrieve the CSV file from GitHub.
2. StardogPut processor that ingests the CSV file into a staging graph in Stardog. It uses the Stardog mappings available in the examples repository.
3. StardogQuery processor that copies the staging graph to the default graph and updates the last modification time.

Follow these steps to upload this workflow to your NiFi instance (see the screencast below and refer to
Apache NiFi user interface for terminology):

1. From the Operate Palette, click the "Upload Template" button and select the `covid19-stardog.xml` file.
2. Drag the "Template" icon from the Components Toolbar onto the canvas.
3. Add a database to your Stardog instance called `covid19`. The CLI command for this is: `bin/stardog-admin db create -n covid19`
4. Unselect the processors by clicking an empty spot on the canvas, and then select the `StardogPut` processor to configure the connection details.
Confirm the Stardog Connection String. Set the username and password. Point to the correct location for the mappings file, `nyt-covid.sms`.
5. Modify the connection details for the `StardogQuery`  processor in a similar way.

![NiFi Demo](/assets/images/nifiexample.gif)

The example is created to run every hour, so if you leave NiFi running, the data will be fetched, transformed,
and uploaded into Stardog every hour.

Instead of supplying the Stardog URL and credentials to every Stardog processor, you can configure the 
Stardog Connection Service once and then reference that service in each Stardog processor.
