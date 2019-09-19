# Image recognizer
Image recognizer is a simple python script use for finding similar or duplicated images in
a path. It uses image-match and sqlite3 for generating and storing the signatures

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

This must be done everytime you change the folder content, the scripts calculates the signatures
only for the new images.

Then we issue the match command
```
  image_recognizer match {path to folder}
```
This will print on the console log which images are visually similar and their similarity distance.
You can parse the output a script or manually delete the duplicate occurrence, some example
scripts are provided in the scripts folder.
