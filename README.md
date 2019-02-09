# Alation_Article

The code in this repository is sample code for migrating articles from one instance of Alation
to another. 

The code is provided as-is and needs to be adapted to your own situation. You need to have a user
name and password for each instance (source and target).

Run "Article Magic.py" as the main file.

The other files get imported automatically.

The AlationInstance module represents a class to log on to an instance, get Articles, etc.

The Article module represents a class to manipulate a collection of articles.

Known issues:

* You can save to CSV, but not yet read from CSV
* You cannot migrate Articles without a custom template
* If a custom field with the same name but different options exists on the target instance,
the code will most likely fail


