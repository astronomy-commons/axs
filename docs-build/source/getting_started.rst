=================
Starting AXS
=================

After you have :doc:`installed AXS <installation>`, you have three options how to run AXS:

- using `pyspark` shell
- from a Python program
- from a Jupyter notebook

All three options are determined by different ways of starting a Spark session, which is required for running AXS.
And in all three options you have to decide which Spark "master" (or the type of Spark cluster) to use. The easiest
way is to run it locally, using `local[n]` "master" (where `n` is the number of tasks you wish to run in parallel). Other ways
are to start a `Standalone Spark cluster <http://spark.apache.org/docs/latest/spark-standalone.html>`__ and connect to it,
or connect to an existing `YARN <hhttp://spark.apache.org/docs/latest/running-on-yarn.html>`__ or
`Mesos <http://spark.apache.org/docs/latest/running-on-mesos.html>`__ cluster. There is also an option to run Spark
in `Kubernetes <http://spark.apache.org/docs/latest/running-on-kubernetes.html>`__.


Starting AXS using pyspark shell
================================
Start `pyspark` specifying `master` and other options as you see fit. For example:

.. code-block:: bash

    pyspark --master=local[10] --driver-memory=64g

Then, inside the shell, create an instance of AXS catalog, providing the `SparkSession` instance already initialized in
the shell:

.. code-block:: python

    from axs import AxsCatalog
    db = AxsCatalog(spark)


Starting AXS using a Python program
===================================
To use AXS from a Python program, first set the `SPARK_HOME` environment variable to the directory where you installed
AXS' version of Spark. For example:

.. code-block:: python

    export SPARK_HOME=/opt/axs-dist

To connect to a Spark Standalone cluster, first start it from the AXS installation directory:

.. code-block:: bash

    sbin/start-all.sh

Then, after you start an ordinary Python shell, you will first need to create an instance of `SparkSession` and connect
to the Spark cluster. For example:

.. code-block:: python

    spark = SparkSession.builder.\
        master("spark://localhost:7078").\
        config("spark.cores.max", "10"). \
        config("spark.executor.cores", "1").\
        config("spark.driver.memory", "12g"). \
        config("spark.executor.memory", "12g"). \
        appName("AXS application").getOrCreate()
    from axs import AxsCatalog
    db = AxsCatalog(spark)

In this example the Spark session is pointing to a Spark standalone master listening at port `7078` at `localhost`.
Of course, you can also specify the master to be `local[10]`, as in the previous example.

Starting AXS from a Jupyter notebook
====================================
To correctly setup a Jupyter environment, it is best to create a new Jupyter kernel and then edit the corresponding
`kernel.json` file. You can get a list of current kernels using the following command:


.. code-block:: bash

    jupyter kernelspec list

Then edit the `kernel.json` file in the dispalyed folder (e.g. `/usr/local/share/jupyter/kernels/mypythonkernel/kernel.json`)
and change the variables in the `env` section. Here as a working example when AXS in installed in `/opt/axs-dist`:

.. code-block:: json

    {
     "argv": [
      "/opt/anaconda/bin/python",
      "-m",
      "ipykernel_launcher",
      "-f",
      "{connection_file}"
     ],
     "display_name": "AXS",
     "language": "python",
     "env": {
      "SPARK_HOME": "/opt/axs-dist",
      "PYLIB": "/opt/axs-dist/python/lib:/opt/spark-axs/python/lib/py4j-0.10.7-src.zip",
      "PYTHONPATH": "/opt/axs-dist/python:/opt/anaconda/lib/python36.zip:/opt/anaconda/lib/python3.6:/opt/anaconda/lib/python3.6/lib-dynload:/opt/anaconda/lib/python3.6
    /site-packages:/opt/anaconda/lib/python3.6/site-packages/IPython/extensions",
      "JUPYTERPATH": "/opt/axs-dist/python:/epyc/opt/anaconda/lib/python36.zip:/opt/anaconda/lib/python3.6:/opt/anaconda/lib/python3.6/lib-dynload:/opt/anaconda/lib/python3.
    6/site-packages:/opt/anaconda/lib/python3.6/site-packages/IPython/extensions",
      "PATH": "/opt/axs-dist/python/lib/py4j-0.10.7-src.zip:/opt/spark-axs/bin:/epyc/opt/jdk1.8.0_161/bin:/opt/anaconda/bin:/usr/lib64/qt-3.3/bin:/bin:/usr/local/sbin:/usr/local/
    bin:/usr/sbin:/usr/bin:/var/lib/jupyterhub/miniconda/bin",
      "JAVA_HOME": "/opt/jdk1.8.0_161"
     }
    }

Then start a new notebook with the kernel you just edited. After that, you can start a Spark session and create an
instance of `AxsCatalog` just as you did in the Python case above.

