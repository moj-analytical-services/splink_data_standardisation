#!/bin/bash

pytest -x --cov-report term-missing --cov=sparklink_data_normalisation tests/

if [ $? -ne 0 ]
then
  exit 1
fi


exit 0