use std::{collections::HashSet, env};

use aws_config::{retry::RetryConfig, Region};
use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};
use aws_sdk_s3::{config::Builder, Client};

async fn get_client(
    env_config_files: EnvConfigFiles,
    region: Region,
    endpoint_url: Option<&str>,
) -> Client {
    let mut config_loader = aws_config::from_env()
        .profile_files(env_config_files)
        .region(region)
        .retry_config(RetryConfig::standard().with_max_attempts(u32::MAX));
    config_loader = match endpoint_url {
        Some(url) => config_loader.endpoint_url(url),
        None => config_loader,
    };
    let config = Builder::from(&config_loader.load().await)
        .force_path_style(true)
        .build();
    Client::from_conf(config)
}

fn region_from_str(region: &str) -> Region {
    match region {
        "us-east-1" => Region::from_static("us-east-1"),
        "ap-northeast-1" => Region::from_static("ap-northeast-1"),
        "ap-northeast-3" => Region::from_static("ap-northeast-3"),
        _ => panic!("Invalid region"),
    }
}

#[tokio::main]
async fn main() {
    let old_client = get_client(
        EnvConfigFiles::builder()
            .with_file(EnvConfigFileKind::Credentials, ".old.credentials")
            .build(),
        region_from_str(
            env::var("OLD_AWS_REGION")
                .unwrap_or("us-east-1".to_string())
                .as_str(),
        ),
        env::var("OLD_AWS_ENDPOINT_URL").ok().as_deref(),
    )
    .await;

    for bucket in old_client
        .list_buckets()
        .send()
        .await
        .unwrap()
        .buckets
        .unwrap_or_default()
    {
        let bucket_name = bucket.name.as_deref().unwrap_or_default();
        let mut objects = HashSet::new();

        let mut list_objects_output = old_client
            .list_objects()
            .bucket(bucket_name)
            .send()
            .await
            .unwrap();

        for object in list_objects_output.contents.unwrap_or_default() {
            objects.insert(object.key.as_deref().unwrap_or_default().to_string());
        }

        while let Some(next_marker) = list_objects_output.next_marker {
            list_objects_output = old_client
                .list_objects()
                .bucket(bucket_name)
                .marker(next_marker)
                .send()
                .await
                .unwrap();

            for object in list_objects_output.contents.unwrap_or_default() {
                objects.insert(object.key.as_deref().unwrap_or_default().to_string());
            }
        }

        for object in objects {
            println!("Deleting object: {}", object);

            old_client
                .delete_object()
                .bucket(bucket_name)
                .key(&object)
                .send()
                .await
                .unwrap();
        }

        old_client
            .delete_bucket()
            .bucket(bucket_name)
            .send()
            .await
            .unwrap();
    }
}
