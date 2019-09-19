# Image recognizer
Image recognizer is a simple python script that finds similar or duplicated images in
specified path. It uses image-match and sqlite3 for generating and storing the signatures

# Installation
```
  git clone https://github.com/Jacotsu/image_recognizer.git
  pip3 install --user image_recognizer
```

# Usage
First we populate the database with the image signatures using the following command
```
  image_recognizer update {path to folder}
```

This must be done every time you change the folder content, the scripts generates the signatures
only for the newfound images.

Then we issue the match command
```
  image_recognizer match {path to folder}
```
This will print on the console log which images are visually similar and their similarity distance.
You can parse the output with a script or manually delete duplicates, some example
scripts are provided in the scripts folder.
