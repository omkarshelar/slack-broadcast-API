# Slack Broadcast APIs

Web Link : https://slack.omkarshelar.dev/


**The application provides management of the users' Slack Channels and enables them to broadcast the same message to multiple Slack Channels using slack webhooks.**

---

#### API Docs :
https://documenter.getpostman.com/view/3370668/SzmcbzVN

#### GitHub Links : 

Angular Frontend : https://github.com/omkarshelar/slack-broadcast-frontend

Lambda Code : (This Repo) https://github.com/omkarshelar/slack-broadcast-API

#### Architectural Diagram :

![Slack Broadcast Architecture](/docs/slack-broadcast-arch-transparent.svg "Slack Broadcast Architecture")


#### Application Features :
* Single Sign On using OAuth.
* Verification of user email.
* Completely Serverless deployment.
* Angular application is a Progressive Web Application.

#### What I learned from the Project :
* Implementing OAuth using Authorization Code Grant flow.
* Using AWS Amplify.
* Using Angular HTTP interceptors.
* Deploying code to AWS Lambda using [AWS Chalice](https://github.com/aws/chalice).
* Designing and Querying DynamoDB tables.

---