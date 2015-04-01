#!/bin/bash

ssh -Cv -L 11985:ela4-app12182.prod.linkedin.com:11984 -L 12914:zk-ela4-pinot.prod:12913 eng-portal  
