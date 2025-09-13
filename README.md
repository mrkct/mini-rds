# rds-lite

This software simulates a minuscule subset of the AWS RDS Data API, running the received statements on a local MySQL database.

Only the ExecuteStatement and BatchExecuteStatement API calls are supported, and only their happy path are supported.
Chances are it's missing something you need.

## Usage

You should first ensure you have a MySQL database that rds-lite can access

```
RUST_LOG=info DATABASE_URL="mysql://root:my-secret-password@localhost:3306" PORT=3000 cargo run
```

You're all set, you can test this using the `aws` cli. For example:

```
AWS_ACCESS_KEY_ID=ABCD \
AWS_SECRET_ACCESS_KEY=EF1234 \
aws rds-data execute-statement --resource-arn "123" --secret-arn "123" --region eu-west-1 --endpoint http://localhost:3000 --database "testdb" --sql "select * from sample_data where id = 1" 
```

Note that most of the parameters we're passing to the `aws' cli tool are useless as they're ignored by mini-rds, but the tool still wants them.
