from dataclasses import dataclass, field


@dataclass
class S3Config:
    bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str = "us-east-1"
    prefix: str = ""
    endpoint_url: str | None = None

    def __post_init__(self):
        # Normalize prefix: no leading slash, trailing slash always present if non-empty
        self.prefix = self.prefix.strip("/")
        if self.prefix:
            self.prefix += "/"

    def s3_base_uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"

    def table_uri(self, table_name: str) -> str:
        return f"{self.s3_base_uri()}{table_name}.parquet"
