# GraalVM Native Image

1. Install GraalVM with native image support - note: this replaces your jdk, which is needed for step 2.
2. Run various geomesa-accumulo CLI commands to update the config files in this directory that GraalVM uses for things
   like reflection, SPI, etc. Run different commands to ensure better code coverage.
3. Re-build the geomesa-accumulo-tools jar to get the updated GraalVM configuration files and install it in the tools lib.
4. Run `geomesa-accumulo native` to generate the native image.
5. Invoke the native image with `geomesa-accumulo-native ...` using standard CLI arguments (export, ingest, etc).
6. ...
7. Profit!
