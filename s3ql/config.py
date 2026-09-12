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

    def table_base_prefix(self, table_name: str) -> str:
        """S3 key prefix for a partitioned table's directory (no leading s3://)."""
        return f"{self.prefix}{table_name}/"

    def table_glob_uri(self, table_name: str, n_part_cols: int) -> str:
        """Glob URI matching all partition data files for a partitioned table."""
        part_pattern = "/".join(["*=*"] * n_part_cols)
        return f"s3://{self.bucket}/{self.prefix}{table_name}/{part_pattern}/part-0.parquet"

    def partition_s3_key(self, table_name: str, part_cols: list[str], part_vals: tuple) -> str:
        """S3 key for a specific partition's data file."""
        path = "/".join(f"{col}={val}" for col, val in zip(part_cols, part_vals))
        return f"{self.prefix}{table_name}/{path}/part-0.parquet"
