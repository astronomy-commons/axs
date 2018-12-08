
=================
AXS Installation
=================

Currently the only supported method of installing AXS is the manual installation. Anaconda installer is planned
for the future.

Prerequisites
===============
Before running AXS make sure you have Java (at least v.8) installed and JAVA_HOME variable set.

You will also need Python 3 with numpy, pandas and arrow packages.

Manual installation
====================
To install AXS follow these steps:

1. Download the latest AXS tarball from the `realeases page <http://github.com/dirac-institute/AXS/releases>`__.
2. Unpack the tarball to a directory of your choosing.
3. Set `SPARK_HOME` environment variable to point to the extraction directory.
4. Add `SPARK_HOME/bin` to your `PATH` variable.
5. Run the `axs-init-config.sh` script to update `spark-defaults.conf` and `hive-site.xml` files with the exact `SPARK_HOME` path.

Using Anaconda
===============


.. Downloading catalog data
  =========================


.. Running in the cloud
  =====================

