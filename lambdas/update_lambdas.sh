#!/bin/bash

rm -rf lambdas/build \
&& pip install pymysql==0.10.1 --target lambdas/build/packages/ \
&& cd lambdas/build/packages \
&& zip -r9 ../get_error_list.zip . \
&& cd ../../.. \
&& cd lambdas/ \
&& zip -g build/get_error_list.zip get_error_list.py \
&& cd .. \
&& aws lambda update-function-code --function-name get_error_list --zip-file fileb://lambdas/build/get_error_list.zip \
&& aws lambda update-function-configuration --function-name get_error_list --handler get_error_list.lambda_handler