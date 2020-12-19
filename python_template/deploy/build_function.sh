#! /bin/bash

# Destroy and prepare build folder
rm -rf build_function
mkdir build_function

# Copy files to build folder.
cp -R ../src/* ./build_function
cp -R ../platforms/aws/* ./build_function

# Zip and submit to AWS Lambda.
cd ./build_function
zip -X -r ./index.zip *
