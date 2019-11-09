#!/bin/bash

flintrock --config conf.yaml launch test-cluster &&
#flintrock --config conf.yaml copy-file test-cluster "wiki_geo_edits.jar" "/home/ec2-user/wiki_geo_edits.jar" &&
flintrock --config conf.yaml run-command test-cluster "mkdir .aws" &&
flintrock --config conf.yaml copy-file test-cluster "/home/enrico/.aws/credentials" "/home/ec2-user/.aws/credentials" &&
flintrock --config conf.yaml run-command test-cluster "aws s3 cp s3://wikigeoedits/IP2LOCATION-LITE-DB9.CSV.bz2 ." &&
flintrock --config conf.yaml run-command test-cluster "aws s3 cp s3://wikigeoedits/enwiki-longIpOnly.bz2 ." &&
flintrock --config conf.yaml run-command test-cluster "aws s3 cp s3://wikigeoedits/Wikipedia-geoEdits-assembly-0.1.jar wiki_geo_edits.jar" &&
flintrock --config conf.yaml run-command test-cluster "aws s3 cp s3://wikigeoedits/catIpsFinal catIpsFinal --recursive"

