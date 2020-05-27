#!/bin/sh

go build
zip on-upload-to-s3.zip on-upload-to-s3 rds-ca-2019-root.pem
aws s3 cp on-upload-to-s3.zip s3://exchangedataset-lambdas/
aws lambda update-function-code --function-name exchangedataset-on-upload-to-s3 --s3-bucket exchangedataset-lambdas --s3-key on-upload-to-s3.zip
