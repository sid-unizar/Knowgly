#!/bin/bash

mvn package
rm ./target/Knowgly-1.0.jar
mv ./target/Knowgly-1.0-Main.jar ./Knowgly.jar
mv ./target/Knowgly-1.0-RunEvaluator.jar ./RunEvaluator.jar