#!/bin/bash

BUILD_DIR="build_package"
PKG_DIR="build_package/python/lib/python3.8/site-packages"

rm -rf $BUILD_DIR && mkdir -p $PKG_DIR 

docker run --rm -v $(pwd):/tmp -w /tmp lambci/lambda:build-python3.8 \
    pip3 install -r requirements.txt --no-deps -t $PKG_DIR

cd $BUILD_DIR
zip -r pymysql.zip .
cd ..
