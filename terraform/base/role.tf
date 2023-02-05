resource "aws_iam_role" "glue_job_role" {
  name_prefix = "glue-job-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

data "aws_iam_policy_document" "glue_job_policy_document" {
  statement {
    actions = [
      "s3:ListBucket"
    ]

    resources = [
      "arn:aws:s3:::${var.datalake_bucket}",
      "arn:aws:s3:::${var.input_bucket}",
      "arn:aws:s3:::${aws_s3_bucket.glue_script_bucket.id}"
    ]
  }

  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "arn:aws:s3:::${var.datalake_bucket}/*",
      "arn:aws:s3:::${var.input_bucket}/*",
      "arn:aws:s3:::${aws_s3_bucket.glue_script_bucket.id}/*"
    ]
  }
}

resource "aws_iam_role_policy" "glue_job_policy" {
  name = "glue-job-policy"
  role = aws_iam_role.glue_job_role.name

  policy = data.aws_iam_policy_document.glue_job_policy_document.json
}

data "aws_iam_policy" "glue_policy" {
  arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_policy_attachment" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = data.aws_iam_policy.glue_policy.arn
}