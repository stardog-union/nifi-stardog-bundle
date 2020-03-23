# Stardog NiFi plugin

This repository contains the Stardog processors for Apache NiFi system.

## Building

Run the following command to build the NiFi .nar file:

```
mvn package
```

Copy the .nar file to NiFi lib directory:

``` 
cp nifi-stardog-nar/target/nifi-stardog-nar-$VERSION.nar  $NIFI_DIR/lib/
```

Restart NiFi for changes to take effect.

