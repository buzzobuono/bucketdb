from dataclasses import dataclass


@dataclass
class S3Config:
    bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str = "us-east-1"
    prefix: str = ""
    endpoint_url: str | None = None

    def __post_init__(self):
        self.prefix = self.prefix.strip("/")
        if self.prefix:
            self.prefix += "/"

    def s3_base_uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"

    def table_prefix(self, table_name: str) -> str:
        """S3 key prefix for all objects belonging to a table."""
        return f"{self.prefix}{table_name}/"

    def meta_key(self, table_name: str) -> str:
        """S3 key for the table's _meta.json file."""
        return f"{self.prefix}{table_name}/_meta.json"

    def file_uri(self, table_name: str, rel_path: str) -> str:
        """Full S3 URI for a file given its path relative to the table prefix."""
        return f"s3://{self.bucket}/{self.prefix}{table_name}/{rel_path}"

    def data_file_key(self, table_name: str, filename: str) -> str:
        """S3 key for a data file inside the table's data/ directory."""
        return f"{self.prefix}{table_name}/data/{filename}"

    def data_file_uri(self, table_name: str, filename: str) -> str:
        return f"s3://{self.bucket}/{self.prefix}{table_name}/data/{filename}"
