Code Requirements
=================

The criteria outlined represent the essential requirements that the code needs to fulfill for successful execution and optimal performance.

.. code-block:: python
    :linenos:

    pip install pep8
    pip install --upgrade pep8

.. code-block:: python
    :linenos:

    # Example following PEP8 style guide
    def calculate_avg(numbers):
        # use Descriptive variable names
        total = sum(numbers)
        count = len(numbers)

        # Spaces in between symbols. Ex: >, <, >=, # etc...
        if count > 0:
            #Caculate average if there are elements in the list
            average = total / count
            return average
        else:
            #return None if the list is empty
            return None

* PEP8
    * Readability: Enforces consistent and clear coding style. Easier for developers to understand and maintain
    * Provides a standard set of conventions that Python programmers can follow. Making it easier to collaborate
    * Tools and automation such as Black 23.3.0 (python install package) can automatically check your code for style violations.
        * I beleive VS Code has an extension that will automatically do this if run say python black myfile.py
    * Using PEP8 can help codebase evolve and grow over time.
* Docstring/Google Docstring
    * Both are the same, however, I personally think Google docstring is cleaner.

.. code-block:: python
    :linenos:

    # This is Docstring
    """
    Return the difference in days between the current date and game release date.

    Parameter
    ---------
    date : str
        Release date in string format.

    Returns
    -------
    int64
        Integer difference in days.
    """

.. code-block:: python
    :linenos:

     # Google docstring
     """Return the difference in days between the current date and game release date.

    Args:
        date (str): Release date in string format.

    Returns:
        int64: Integer difference in days.
    """