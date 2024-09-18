# AWS Lambda Football scraper

## Information


This is a personal project I built to pull up-to-date Premier League data on all current players. 

It does this by parsing the HTML of a website for the data and processing it using pandas for easy manipulation.

The project is invoked in production using a AWS Lambda function which uploads the data to an S3 bucket and copies it to another S3 bucket. I then take one of these buckets, create a presigned URL and then emails it to myself and a couple friends every week. This is all coordinated by using AWS Event Bridge and AWS ECR to deploy and run the Lambda in a containerized environnment to ensure consistency.

*Note: This repo is not structured as AWS Lambda flattens the file structure anyways*


![alt text](<screenshot.png>)



## How to run

This project can be built in a docker using `docker build .` and using environment variables for: 
- USER - username for gmail
- PASSWORD - password for gmail
- WEBSITE_NAME - name of the website (this is hidden since i don't want people to use this really, but it's possible to deduce it)
- RECEIPIENTS - receipients of the email in a string with each email seperated by a comma


Optionally, you can just put this into a Lambda in your AWS account and configure the env variables and this would work also.


