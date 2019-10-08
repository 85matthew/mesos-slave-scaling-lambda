#!/bin/bash
set -e

APPLICATION_NAME="mesosslave-scaledown.zip"
BUCKET_PATH="mybucket/myprefix/lambda/"
echo $APPLICATION_NAME
rm -rf project
mkdir project
cp lambda_function.py project/
pip install requests t project
pip install boto3 -t project

(cd project
zip -r ${APPLICATION_NAME} .

# Copy to your S3 bucket as artifact
#aws s3 cp ${APPLICATION_NAME} s3://$BUCKET_PATH
echo "Lambda function uploaded to S3"
)
echo "Lambda function uploaded to S3"

echo "please resubmit \"https://s3.amazonaws.com/$BUCKET_PATH/${APPLICATION_NAME}\" through the UI"
