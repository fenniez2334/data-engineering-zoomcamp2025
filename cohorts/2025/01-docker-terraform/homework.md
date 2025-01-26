# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework. 

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.


## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1
- 24.2.1
- 23.3.1
- 23.2.1

### Question 1 Answer:

To run the docker, run the command `docker run -it --entrypoint=bash python:3.12.8`

To get the `pip` version in the image, run `pip --version`

>My answer:
```
24.3.1
```


## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

If there are more than one answers, select only one of them

### Question 2 Answer:

The `hostname` is `db`, the `port` is `5432`.
```
db:5432
```

##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

### Prepare Postgres Answer:

Run Postgres and load data as shown in the lecture videos

>Command:
```bash
# from the working directory where docker-compose.yaml is
docker-compose up
```

>Command:
```bash
# download green taxi trips from October 2019
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
# unzip the file
gunzip green_tripdata_2019-10.csv.gz
# download zone dataset
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

>Command:
```bash
# Create a new ingest script that ingests both files called ingest_data.py, then dockerize it with
docker build -t taxi_ingest:homework .

# Now find the network where the docker-compose containers are running with
docker network ls

# Finally, run the dockerized script
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
URL2="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
docker run -it \
    --network=01-docker-terraform_default  \
    taxi_ingest:homework \
    --user=postgres \
    --password=postgres \
    --host=db \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --table_name2=zones \
    --url=${URL} \
    --url2=${URL2}
```


## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles 

Answers:

- 104,802;  197,670;  110,612;  27,831;  35,281
- 104,802;  198,924;  109,603;  27,678;  35,189
- 104,793;  201,407;  110,612;  27,831;  35,281
- 104,793;  202,661;  109,603;  27,678;  35,189
- 104,838;  199,013;  109,645;  27,688;  35,202

### Question 3 Answer:

>Command:
```sql
SELECT
  CASE 
    WHEN trip_distance <= 1 THEN 'Up to 1 mile'
	  WHEN trip_distance > 1 AND trip_distance <= 3 THEN 'In between 1 and 3 miles'
	  WHEN trip_distance > 3 AND trip_distance <= 7 THEN 'In between 3 and 7 miles'
	  WHEN trip_distance > 7 AND trip_distance <= 10 THEN 'In between 7 and 10 miles'
	  ELSE 'Over 10 miles'
  END AS distance_category,
  COUNT(*) AS trip_count
FROM 
  green_taxi_trips
WHERE
  (lpep_dropoff_datetime>='2019-10-01 00:00:00' AND
  lpep_dropoff_datetime<'2019-11-01 00:00:00')
GROUP BY
  distance_category;
```
>Answer (correct):
```
"Up to 1 mile"	104802
"In between 1 and 3 miles"	198924
"In between 3 and 7 miles"	109603
"In between 7 and 10 miles"	27678
"Over 10 miles"	35189
My answer are 104,802;  198,924;  109,603;  27,678;  35,189;
```


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31

### Question 4 Answer:

>Command:
```sql
SELECT
  lpep_pickup_datetime
FROM 
  green_taxi_trips
WHERE
  trip_distance = (
    SELECT 
      MAX(trip_distance)
    FROM 
      green_taxi_trips
    );
```
>Answer (correct):
```
2019-10-31
```


## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
 
- East Harlem North, East Harlem South, Morningside Heights
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park

### Question 5 Answer:

>Command:
```sql
SELECT
  t."PULocationID",
  z."Zone",
  SUM(t.total_amount) AS total_amount_sum,
  Count(t."PULocationID") AS zone_count
FROM 
  green_taxi_trips AS t
LEFT JOIN 
  zones AS z
ON t."PULocationID" = z."LocationID"
WHERE
  lpep_pickup_datetime >= '2019-10-18 00:00:00' AND
  lpep_pickup_datetime < '2019-10-19 00:00:00'
GROUP BY
  t."PULocationID",
  z."Zone"
HAVING
  SUM(t.total_amount) > 13000
ORDER BY
  zone_count DESC
```
>Answer (correct):
```
"PULocationID"	"Zone"	"total_amount_sum"	"zone_count"
74	"East Harlem North"	18686.680000000084	1236
75	"East Harlem South"	16797.260000000057	1101
166	"Morningside Heights"	13029.79000000003	764
My answer: East Harlem North, East Harlem South, Morningside Heights
```


## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone
named "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- Yorkville West
- JFK Airport
- East Harlem North
- East Harlem South

### Question 6 Answer:

>Command:
```sql
SELECT
  t."PULocationID",
  z."Zone" AS pickup_zone,
  t."DOLocationID",
  z2."Zone" AS dropoff_zone,
  t.tip_amount
FROM 
  green_taxi_trips AS t
LEFT JOIN 
  zones AS z
ON t."PULocationID" = z."LocationID"
LEFT JOIN
  zones AS z2
ON t."DOLocationID" = z2."LocationID"
WHERE
  z."Zone" = 'East Harlem North'
ORDER BY
  tip_amount DESC
LIMIT 1;
```
>Answer (correct):
```
"PULocationID"	"pickup_zone"	"DOLocationID"	"dropoff_zone"	"tip_amount"
    74	        "East Harlem North"	132	          "JFK Airport"	     87.3
My answer: JFK Airport
```


## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

## My Answer for Terraform

Install terraform and go to the terraform directory (`data-engineering-zoomcamp/01-docker-terraform/1_terraform_gcp/terraform/terraform_with_variables`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output (after running `apply`) to the form
>Answer:
```
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be created
  + resource "google_bigquery_dataset" "demo_dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "demo_dataset"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = {
          + "goog-terraform-provisioned" = "true"
        }
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "US"
      + max_time_travel_hours      = (known after apply)
      + project                    = "central-beach-447906-q6"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = {
          + "goog-terraform-provisioned" = "true"
        }

      + access (known after apply)
    }

  # google_storage_bucket.demo-bucket will be created
  + resource "google_storage_bucket" "demo-bucket" {
      + effective_labels            = {
          + "goog-terraform-provisioned" = "true"
        }
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "US"
      + name                        = "central-beach-447906-q6-terra-bucket"
      + project                     = (known after apply)
      + project_number              = (known after apply)
      + public_access_prevention    = (known after apply)
      + rpo                         = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = {
          + "goog-terraform-provisioned" = "true"
        }
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type          = "AbortIncompleteMultipartUpload"
                # (1 unchanged attribute hidden)
            }
          + condition {
              + age                    = 1
              + matches_prefix         = []
              + matches_storage_class  = []
              + matches_suffix         = []
              + with_state             = (known after apply)
                # (3 unchanged attributes hidden)
            }
        }

      + soft_delete_policy (known after apply)

      + versioning (known after apply)

      + website (known after apply)
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/central-beach-447906-q6/datasets/demo_dataset]
google_storage_bucket.demo-bucket: Creation complete after 1s [id=central-beach-447906-q6-terra-bucket]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```

Finally, run `terraform destroy` to remove all resources created by terraform.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy
- terraform import, terraform apply -y, terraform rm

### Question 7 Answer:

>My answer:
```
terraform init, terraform apply -auto-approve, terraform destroy
```


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw1
