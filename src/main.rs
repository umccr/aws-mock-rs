use std::error::Error;

use aws_config;
use aws_sdk_s3::{Client};
use aws_sdk_s3 as s3;

// pub struct ListObjectsResult {
//     pub objects: Vec<s3::model::Object>,
//     pub continuation_token: Option<String>,
//     pub has_more: bool,
// }

// #[async_trait]
// pub trait ListObjects {
//     async fn list_objects(
//         &self,
//         bucket: &str,
//         prefix: &str,
//         continuation_token: Option<String>,
//     ) -> Result<ListObjectsResult, Box<dyn Error + Send + Sync + 'static>>;
// }

// Lists all objects in an S3 bucket with the given prefix, and adds up their size.
async fn determine_prefix_file_size(
    s3: s3::Client,
    bucket: &str,
    prefix: &str,
) -> Result<usize, Box<dyn Error + Send + Sync + 'static>> {
    let mut next_token: Option<String> = None;
    let mut total_size_bytes = 0;
    loop {
        let response = s3
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .set_continuation_token(next_token.take())
            .send()
            .await?;

        // Add up the file sizes we got back
        if let Some(contents) = response.contents() {
            for object in contents {
                total_size_bytes += object.size() as usize;
            }
        }

        // Handle pagination, and break the loop if there are no more pages
        next_token = response.continuation_token().map(|t| t.to_string());
        if !response.is_truncated() {
            break;
        }
    }
    Ok(total_size_bytes)
}

#[tokio::main]
async fn main() {
    let shared_config = aws_config::load_from_env().await;
    let client = Client::new(&shared_config);
    println!("{}", determine_prefix_file_size(client, "umccr-research-dev", "htsget").await.unwrap());
}
