## Implement Amazon RDS for SQL Server Standard edition cross-Region disaster recovery using access to transaction log backups feature

Detailed documentation of this solution is published as blog and available at the following link:

https://aws.amazon.com/blogs/database/implement-amazon-rds-for-sql-server-standard-edition-cross-region-disaster-recovery-using-access-to-transaction-log-backups-feature/

## Solution overview

For our use case, we take a scenario in which our production Amazon RDS for SQL Server instance is configured in high availability mode using multiple Availability Zones, and AWS Key Management Service (AWS KMS) is used for data at rest encryption. Our DR Amazon RDS for SQL Server instance is configured in a single Availability Zone to save costs. Both production and DR instances are using SQL Server Standard edition. We also want to keep the recovery point objective (RPO) as low as possible with a design where production and DR RDS SQL Server instances are loosely coupled using a tracking server model supported by Amazon RDS SQL Server Express, a free edition of SQL Server to keep the costs low. In our solution, we use the following key features: 

*	Amazon S3 Cross-Region Replication (CRR)
*	Amazon S3 Replication Time Control (S3 RTC)
*	AWS KMS multi-Region keys
*	Amazon RDS for SQL Server Agent job replication

This solution involves the creation and utilization of new AWS resources. Therefore, it will incur costs on your account. Refer to AWS Pricing for more information. We strongly recommend that you set this up in a non-production instance and run the end-to-end validations before you implement this solution in a production environment.

The following diagram illustrates our solution architecture.

![image](https://user-images.githubusercontent.com/96596850/218197106-4ced9a92-b622-46e1-b96d-2911b90c5cc1.png)

To implement the solution, we run the following high-level setup steps as outlined in the blog:

1.	Create an S3 bucket and add a bucket policy.
2.	Create a cross-Region bucket replication rule.
3.	Create a multi-Region KMS key.
4.	Create an AWS Identify and Access Management (IAM) policy and role and add permissions.
5.	Create an option group and add a native backup and restore option.
6.	Create Production, DR, and Tracking Amazon RDS for SQL Server instances.
7.	Enable transaction log backup copy at Production.
8.	Create sample databases at Production.
9.	Stage the solution.
10.	Implement the solution.


## License

This library is licensed under the MIT-0 License. See the LICENSE file.

