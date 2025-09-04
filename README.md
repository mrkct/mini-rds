AWS_ACCESS_KEY_ID=ABCD AWS_SECRET_ACCESS_KEY=EF1234 aws rds-data execute-statement --resource-arn "arn:aws:rds:us-east-1:123456789012:cluster:mydbcluster" --database "testdb" --secret-arn "arn:aws:secretsmanager:us-east-1:123456789012:secret:mysecret" --sql "select * from sample_data where id = 1" --region eu-west-1 --endpoint http://localhost:3000

