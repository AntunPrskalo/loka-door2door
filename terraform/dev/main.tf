terraform {
  backend "s3" {
    bucket         = "terraform-door2door"
    key            = "terraform.tfstate"
    region         = "eu-central-1"
    dynamodb_table = "terraform-door2door"
  }
}

module "base" {
  source = "../base"

    input_bucket     = "de-tech-assessment-2022"
    input_bucket_key = "data/"

    datalake_bucket     = "door2door-datalake"
    datalake_bucket_key = "vehicle_iot_data/"

    data_catalog_database = "door2door-datacatalog"
    data_catalog_table    = "vehicle_iot_data"
}
