# Marketing-Analytics

### To run the tests:

1. In IntelliJ IDEA Run 'AppSuite', no other actions required.

### To run the tasks:

If you **have** data on your machine: 

0. Create directories `/data/mobile_app_clickstream` and `/data/mobile_app_clickstream` in project's root
1. Move mobile_app_clickstream data files to `.../<project-name>/data/mobile_app_clickstream/` so it would look like: `mobile_app_clickstream/mobile_app_clickstream_1.csv.gz` and so on
2. Move user_purchases data files to `.../<project-name>/data/user_purchases/` so it would look like: `user_purchases/user_purchases_1.csv.gz` and so on

If you **don't have** data on your machine:

1. Set `requires-download = true` in `/main/resources/application.conf` 
2. Set `access-key` and `secret-key` in `/main/resources/application.conf`, according to your S3 credentials

Configure CLI arguments in IntelliJ IDEA:

1. Set `--output=...` (`--output=console` to make output to console or `--output=/<required-path>/` to make output to a file)
2. Set `--task=...` (according to a required task)

Run the tasks:

1. To run task 1 set `--task=convert-csv-parquet` and Run 'Main' from IntelliJ IDEA
2. To run task 2 set `--task=session-projection` and Run 'Main' from IntelliJ IDEA
3. To run task 3.1 set `--task=top-10-campaigns` and Run 'Main' from IntelliJ IDEA
4. To run task 3.2 set `--task=top-channels` and Run 'Main' from IntelliJ IDEA


## Domain
  You work at a data engineering department of a company building an ecommerce platform. There is a mobile application that is used by customers to transact with its on-line store. Marketing department of the company has set up various campaigns (e.g. “Buy one thing and get another one as a gift”, etc.)  via different marketing channels (e.g. Google / Yandex / Facebook Ads, etc.).
  Now the business wants to know the efficiency of the campaigns and channels.

#### Mobile App clickstream projection (mobile_app_clickstream/ )
Schema:
- userId: String
- eventId: String
- eventTime: Timestamp
- eventType: String
- attributes: Option[Map[String, String]]

There could be events of the following types that form a user engagement session:

- app_open
- search_product
- view_product_details
- purchase
- app_close

Events of app_open type may contain the attributes relevant to the marketing analysis:
- campaign_id
- channel_id

Events of purchase type contain purchase_id attribute.

Notes:
1) User session starts  with app_open and closes with app_close. There aren’t sessions with app_open and without app_close. One user is able to have several sessions. Sessions don't intersect with each other.
2) Recommendation for attribute session_id: should be generated using methods that create some unique value, for instance UUID.

Purchases projection (user_purchases/ )

Schema:
- purchaseId: String
- purchaseTime: Timestamp
- billingCost: Double
- isConfirmed: Boolean

## Tasks
### 1. Clear and prepare raw data

### 2. Prepare structured data: build Purchases Attribution Projection

   The projection is dedicated to enabling a subsequent analysis of marketing campaigns and channels. 

The target schema:
- purchaseId: String
- purchaseTime: Timestamp
- billingCost: Double
- isConfirmed: Boolean

   a session starts with app_open event and finishes with app_close
- sessionId: String 

   In order to create sessionId:
  * Add a column with UUID after aggregation by using an udf function
  * Add a column with UUID in the aggregate function
  
- campaignId: String  // derived from app_open#attributes#campaign_id
- channelIid: String    // derived from app_open#attributes#channel_id

### 3. Calculate Marketing Campaigns And Channels Statistics

Use the purchases-attribution projection to build aggregates that provide the following insights:

#### 3.1 Top Campaigns
- What are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)?

#### 3.2 Channels engagement performance
- What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)  with the App in each campaign?
