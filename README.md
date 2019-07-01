# Welcome to AXS
Astronomy Extensions for Spark, or AXS, is a distributed framework for 
astronomical data processing based on Apache Spark. AXS provides simple 
Python API to enable fast cross-matching, querying and analysis of data
from astronomical catalogs.

# Prerequisites
Before running AXS make sure you have Java v8 installed and JAVA_HOME variable set.

You will also need Python 3 with numpy, pandas and arrow packages.

# Installing
To install AXS follow these steps (note that Anaconda installer is planned
for the future):
1. Download the latest AXS tarball from the [realeases page](https://github.com/dirac-institute/AXS/releases).
2. Unpack the tarball to a directory of your choosing.
3. Set `SPARK_HOME` environment variable to point to the extraction directory.
4. Add `SPARK_HOME/bin` to your `PATH` variable.
5. Run the `axs-init-config.sh` script to update `spark-defaults.conf` and `hive-site.xml` files with the exact `SPARK_HOME` path.

And you're good to go!

# Further reading
Read more about starting and using AXS, and its architecture, in 
the [documentation](https://dirac-institute.github.io/AXS).

If you are using AXS in your scientific work, please cite [this paper](https://doi.org/10.3847/1538-3881/ab2384).
