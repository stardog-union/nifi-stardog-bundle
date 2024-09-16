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

## Releasing

When releasing version `$VERSION`, follow these steps:

1. Checkout the master branch: `git checkout master`
1. Run the release command:

``` 
mvn --batch-mode clean release:clean release:prepare -DreleaseVersion=$VERSION
```

2. Go to the [Releases](https://github.com/stardog-union/nifi-stardog-bundle/releases) page and click "Draft a new release"
3. Enter the version number as the "Tag version" and the "Release title"
4. Upload the file `nifi-stardog-processors/target/nifi-stardog-processors-$VERSION.nar` to the "Attach binaries" section
5. Click "Publish release".
6. Run `git push origin master`

Note that, step 2 creates a local tag that will clash with the tag manually created at step 3. You can delete the local tag.