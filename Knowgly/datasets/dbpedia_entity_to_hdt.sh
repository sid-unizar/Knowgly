#!/bin/bash

# Bulk loading of downloaded compressed DBpedia 2015-10 files to HDT (dbpedia-entity.hdt)
# Note: If it causes OOMs, you can edit ./hdt-java-package-3.0.9/bin/javaenv.sh,
#       and increases Java's max heap size under JAVA_OPTIONS
#       Example
#if [ "$JAVA_OPTIONS" = "" ] ; then
#   JAVA_OPTIONS="-Xmx16g"
#fi


FILES="./dbpedia-entity/*.bz2"
for f in $FILES
do
    ./hdt-java-package-3.0.9/bin/rdf2hdt.sh -cattree "$f" "$f.hdt" 
done

hdts=($( ls dbpedia-entity/*.hdt ))
hdtsJoined=$(IFS=' ' ; echo "${hdts[*]}")
./hdt-java-package-3.0.9/bin/hdtCat.sh "$hdtsJoined" dbpedia-entity.hdt
