Diagram
=======

.. blockdiag::
   :desctable:
   :align: center

   blockdiag {
      A -> B -> C;
      A [description = "browsers in each client"];
      B [description = "web server."];
      C [description = "database server"];
   }


.. blockdiag::

    blockdiag {
    // Set stacked to nodes.
    stacked [stacked];
    diamond [shape = "diamond", stacked];
    database [shape = "flowchart.database", stacked];

    stacked -> diamond -> database;
    }

