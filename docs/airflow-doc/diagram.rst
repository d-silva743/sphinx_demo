Diagram
===============

.. blockdiag::
   :caption: Style attributes to nodes and edges (Block Diagram example)
   :align: center
   :width: 640

    {
        // Set boder-style, backgroun-color and text-color to nodes.
        A [style = dotted];
        B [style = dashed];
        C [color = pink, style = "3,3,3,3,15,3"]; //dashed_array format style
        D [shape = circle, color = "#888888", textcolor="#FFFFFF"];

        // Set border-style and color to edges.
        A -> B [style = dotted];
        B -> C [style = dashed];
        C -> D [color = "red", style = "3,3,3,3,15,3"]; //dashed_array format style

        // Set numbered-badge to nodes.
        E [numbered = 99];

        // Set background image to nodes (and erase label).
        F [label = "", background = "https://github.com/sphinx-doc/sphinx/raw/master/doc/_static/sphinx.png"];
        G [label = "", background = "https://www.python.org/static/community_logos/python-logo-master-v3-TM.png"];
        H [icon = "https://github.com/blockdiag/blockdiag.com/raw/master/sources/en/_static/help-browser.png"];
        I [icon = "https://github.com/blockdiag/blockdiag.com/raw/master/sources/en/_static/internet-mail.png"];
        J [shape = actor]

        // Set arrow direction to edges.
        E -> F [dir = none, label = edge];
        F -> G [dir = forward];
        G -> H [dir = back];

        group {
            orientation = portrait;
            color = lightgray;
            H -> I [dir = both];
        }

        // Set width and height to nodes.
        K [width = 192]; // default value is 128
        L [shape = square, height = 64]; // default value is 40

        // Use thick line
        J -> K [thick]
        K -> L;
    }

.. uml::

   @startuml
   
   start
   
   :first activity;
   
   :second activity
    with a multiline 
    and rather long description;
   
   :another activity;
   
   note right
     After this activity,
     are two 'if-then-else' examples. 
   end note
   
   if (do optional activity?) then (yes)
      :optional activity;
   else (no)
   
      if (want to exit?) then (right now!)
         stop
      else (not really)
      
      endif
   
   endif   
      
   :third activity;
   
   note left
     After this activity,
     parallel activities will occur. 
   end note
   
   fork
      :Concurrent activity A;
   fork again
      :Concurrent activity B1;
      :Concurrent activity B2;
   fork again
      :Concurrent activity C;
      fork
      :Nested C1;
      fork again
      :Nested C2;
      end fork
   end fork
   
   repeat 
      :repetitive activity;
   repeat while (again?)
   
   while (continue?) is (yes, of course)
     :first activity inside the while loop;
     :second activity inside the while loop;
   endwhile (no)
   
   stop
   
   @enduml