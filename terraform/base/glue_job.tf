resource "aws_s3_bucket" "glue_script_bucket" {
  bucket = "dooor2door-glue-data"
}

resource "aws_s3_object" "script" {
  bucket = aws_s3_bucket.glue_script_bucket.id
  key    = "scripts/sync.py"
  source = "${path.module}/../../glue/sync.py"
  etag   = filemd5("${path.module}/../../glue/sync.py")
}

resource "aws_glue_job" "glue_job" {
    name         = "vehicle-iot-data-sync"
    glue_version = "4.0"
    role_arn     = aws_iam_role.glue_job_role.arn
    description  = "Glue Job used for copying Door2Door Vehicle IOT data from RAW input bucket into the Data Lake."
    max_retries  = 0

    command {
        script_location = "s3://${aws_s3_bucket.glue_script_bucket.id}/scripts/sync.py"
    }

    default_arguments = local.glue_job_arguments
}

resource "aws_glue_trigger" "glue_job_trigger" {
  name     = "glue_job_trigger"
  schedule = "cron(0 1 * * ? *)"
  type     = "SCHEDULED"

  actions {
        job_name  = aws_glue_job.glue_job.name
        arguments = local.glue_job_arguments
    }
}