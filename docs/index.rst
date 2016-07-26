.. Amazon Distributed Runner documentation master file, created by
   sphinx-quickstart on Tue Jul 26 13:54:46 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Amazon Distributed Runner's documentation!
=====================================================

The Amazon Distributed Runner (ADR) is a simple queueing system based
on the Amazon SQS Message Queue and the Amazon S3 Buckets to handle
batches of numerical model runs or other on-demand processes. Any node
with the ADR installed can process jobs from the central queue. The
ADR provides tools to launch and manage nodes at the Amazon EC2
computational facility, but the queueing system is not restricted to
this facility.

The ADR is available through the OpenEarth GitHub repository:
http://github.com/openearth/amazon-distributed-runner/

Examples
--------

.. code::

   adr config
   adr create
   adr launch -n 5
   adr prepare
   adr queue ~/GitHub/aeolis-models/nickling1995/nickling1995_*.txt
   adr queue ~/GitHub/aeolis-models/dong2004/dong2004_*.txt
   adr download ~/Downloads/
   adr destroy
   
Source code
-----------

.. toctree::
   :maxdepth: 2

   sourcecode

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

