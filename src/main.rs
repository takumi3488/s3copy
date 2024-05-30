use std::env;

use aws_config::Region;
use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};
use aws_sdk_s3::{config::Builder, Client};

async fn get_client(
    env_config_files: EnvConfigFiles,
    region: Region,
    endpoint_url: Option<&str>,
) -> Client {
    let mut config_loader = aws_config::from_env()
        .profile_files(env_config_files)
        .region(region);
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

    let new_client = get_client(
        EnvConfigFiles::builder()
            .with_file(EnvConfigFileKind::Credentials, ".new.credentials")
            .build(),
        region_from_str(
            env::var("NEW_AWS_REGION")
                .unwrap_or("us-east-1".to_string())
                .as_str(),
        ),
        env::var("NEW_AWS_ENDPOINT_URL").ok().as_deref(),
    )
    .await;

    let buckets = old_client
        .list_buckets()
        .send()
        .await
        .unwrap()
        .buckets
        .unwrap();

    for bucket in buckets {
        let bucket_name = bucket.name.as_deref().unwrap();
        println!("Bucket: {}", bucket_name);

        let objects = old_client
            .list_objects()
            .bucket(bucket_name)
            .send()
            .await
            .unwrap()
            .contents
            .unwrap();

        let _ = new_client
            .create_bucket()
            .bucket(bucket_name)
            .send()
            .await;

        for object in objects {
            let object_key = object.key.as_deref().unwrap();
            println!("Object: {}", object_key);

            let object = old_client
                .get_object()
                .bucket(bucket_name)
                .key(object_key)
                .send()
                .await
                .unwrap();

            new_client
                .put_object()
                .bucket(bucket_name)
                .key(object_key)
                .body(object.body.into())
                .send()
                .await
                .unwrap();
        }
    }

    println!("Done!");
}
