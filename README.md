# Alation_Article

The code in this repository is sample code for migrating articles from one instance of Alation
to another. 

The code is provided as-is and needs to be adapted to your own situation. You need to have a user
name and password for each instance (source and target).

Run "Article Magic.py" as the main file. As command line parameters, provide host name, user name and password
for source and target instance. Inspect the code to see what the command line
switches are. (Remember -- this is sample code)

The other files get imported automatically.

The AlationInstance module represents a class to log on to an instance, get Articles, etc.

The Article module represents a class to manipulate a collection of articles.

One of the main complexities of this code is to re-calculate references from the body of the article to another article, or from a parent to a child. These references need to be provided as numbers, but the number is not known before creating the article for the first time. Therefore, at least two passes are necessary.

Known issues:

* You cannot migrate Articles without a custom template
* If a custom field with the same name but different options exists on the target instance,
the code will most likely fail
* References from custom fields (e.g. object sets) are not re-calculated
* In-line graphics will download to a local drive, but to use secure_copy you need a .pem file

The media files are typically located at /data/site_data/media/image_bank/
The directory only gets created once you create the first in line image in an article,
so don't be surprised if the folder does not exist on the target.


