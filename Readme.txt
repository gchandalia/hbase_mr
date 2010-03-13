Provides utilities to read and write Hbase tables from Hadoop -
  - read from Hbase tables with the ability to split a region across several map
    tasks.
  - write to Hbase tables with retries during commits.

To get started, type 
  ant -Dhadoop.jar=<path_to_hadoop_jar> -Dhbase.jar=<path_to_hbase_jar>
in current directory to compile and build the application jar and javadocs.

Dependencies -
  - commons-logging-1.1.1.jar (included in lib/).
  - hadoop core jar (>= 0.20.1), not included, specify through -Dhadoop.jar.
  - hbase jar (>= 0.20.1), not included, specify through -Dhbase.jar.

TODO -
  - Completely port over to the new Hadoop and Hbase interfaces.
