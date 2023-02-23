# Enterprise Data Platform - Open-Source Project
Welcome to the open-source Enterprise Data Platform project! This project aims to provide a core enterprise system that includes a data structure to address data commonly encountered in every business system like contacts, finances, assets, human resource, and activities.
The platform is built on four core principles: Core Data Models, Globally Unique Primary Keys, Record Governance, and Data Transfer. Implementing these principles can help us create enterprise systems that have an inherent capability to exchange data and aggregate data for reporting and AI.

## Why Contribute?
Integrating business software systems has become a growing challenge in recent years, as the demand for data aggregation in AI rises. This project provides a solution to this problem by creating a standardized data structure that can be extended to meet the unique needs of individual organizations. Contributing to this project can help organizations securely exchange data with their suppliers, customers, and partners.

## Getting Started
To get started with the project, follow these steps:
1.	Download the latest version of Postgres.
2.	Clone the project repository from GitHub.
3.	Download the test databases from the project's GitHub repository.
4.	Open a command prompt and navigate to the folder where you downloaded the test databases.
5.	Use the following command to create a new database in Postgres:
createdb -U [USERNAME] [DATABASE_NAME] 
Replace [USERNAME] with your Postgres username and [DATABASE_NAME] with the name of the database you want to create.
6.	Restore the test database from the pgdump file using the following command:
pg_restore -U [USERNAME] -d [DATABASE_NAME] [PATH_TO_PG_DUMP_FILE] 
Replace [USERNAME] with your Postgres username, [DATABASE_NAME] with the name of the database you created in step 5, and [PATH_TO_PG_DUMP_FILE] with the path to the downloaded pgdump file.
7.	Repeat steps 5-6 for each of the test databases.
8.	Run the test scripts to see how the platform operates.

## Documentation
Breaking Bad with 3D Enterprise Systems is a book that explores the challenges of integrating business software systems and proposes a new approach to creating systems based on a core enterprise platform. The book is written by Blair Kjenner and Kewal Dhariwal.  It provides a detailed explanation of the principles and concepts behind the platform.

To complement the book, www.3d-ess has been launched which includes includes 25 training videos and additional resources to help developers get started with the platform.

If you're interested in contributing to the project, we encourage you to check out the website and familiarize yourself with the concepts and principles behind the platform. You can also read the book to gain a deeper understanding of the challenges of integrating business software systems and how the platform addresses those challenges.

## Contributing
We welcome all contributions to this project! To get started, fork the repository and make your changes. Once you have made your changes, submit a pull request to merge your changes into the main branch.
Please ensure that your changes adhere to the project's coding standards and include appropriate tests.
If you have any questions or need help getting started, please reach out to us in the Issues section of the project repository.

## Conclusion
Thank you for your interest in the Enterprise Data Platform project! We hope this project can help organizations overcome the integration challenges of complex conglomerates like the government and allow them to securely exchange data with their suppliers, customers, and partners.
