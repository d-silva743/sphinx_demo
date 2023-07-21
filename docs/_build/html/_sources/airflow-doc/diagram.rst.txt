Diagram
===============

.. blockdiag::
    :desctable:

    blockdiag admin {
      top_page -> config -> config_edit -> config_confirm -> top_page;
    }


.. blockdiag::
   :desctable:
   :align: center

   blockdiag {
      A -> B -> C;
      A [description = "browsers in each client"];
      B [description = "web server"];
      C [description = "database server"];
   }


.. blockdiag::
   :desctable:
   :align: center

    blockdiag {
    // Set stacked to nodes.
    stacked [stacked];
    diamond [shape = "diamond", stacked];
    database [shape = "flowchart.database", stacked];

    stacked -> diamond -> database;
    }

