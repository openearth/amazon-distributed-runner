# amazon-distributed-runner

Distributed model runner based on Amazon SQS Message Queue and Amazon S3 Bucket storage. The runner is compatible with Amazon EC2 computational cloud, but does not depend on it. The package provides a command-line interface to stage and announce modeling jobs that can be processed by a flexible worker pool.

Full documentation can be found at http://adr.readthedocs.io/

## Examples

.. code::

  adr config
  adr create
  adr launch -n 5
  adr prepare
  adr queue ~/GitHub/aeolis-models/nickling1995/nickling1995_*.txt
  adr queue ~/GitHub/aeolis-models/dong2004/dong2004_*.txt
  adr download ~/Downloads/
  adr destroy
