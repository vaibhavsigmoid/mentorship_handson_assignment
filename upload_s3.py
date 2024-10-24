# upload_s3.py
import boto3
from io import StringIO

def upload_to_s3(pandas_df, bucket, file_path, aws_access_key_id, aws_secret_access_key):
    # Initialize the AWS session
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3_client = session.client('s3')

    # Convert DataFrame to CSV in-memory
    csv_buffer = StringIO()
    pandas_df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=file_path,
        Body=csv_buffer.getvalue()
    )
    print(f"File uploaded to S3: {file_path}")
