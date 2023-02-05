locals {
    glue_job_arguments = {
        "--input_bucket"                 = var.input_bucket
        "--input_bucket_key"             = var.input_bucket_key
        "--output_datacatalog_database"  = var.data_catalog_database
        "--output_datacatalog_table"     = var.data_catalog_table
    }
}


variable "input_bucket" {
  description = "Name of the input bucket which holds raw data."
  type        = string
}

variable "input_bucket_key" {
  description = "Folder name which holds the input data."
  type        = string
}

variable "datalake_bucket" {
  description = "Output Data Lake bucket name."
  type        = string
}

variable "datalake_bucket_key" {
  description = "Output Data Lake folder name."
  type        = string
}

variable "data_catalog_database" {
  description = "Name of the database which will hold Data Lake table definitions."
  type        = string
}

variable "data_catalog_table" {
  description = "Name of the database which will hold Vehicle IOT data table definition."
  type        = string
}