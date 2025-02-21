use std::{collections::HashSet, env, mem::take};

use anyhow::Result;
use aws_config::{retry::RetryConfig, Region};
use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};
use aws_sdk_s3::{
    config::Builder,
    operation::{get_object::GetObjectOutput, upload_part::UploadPartOutput},
    types::{
        BucketLocationConstraint, CompletedMultipartUpload, CompletedPart,
        CreateBucketConfiguration, Object,
    },
    Client,
};

const MAX_KEYS: i32 = 1000000;
const CHUNK_SIZE: usize = 5 * 1024 * 1024; // 5MB

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

        let mut new_bucket_name = bucket_name.to_string();

        if let Err(e) = new_client
            .create_bucket()
            .bucket(&new_bucket_name)
            .send()
            .await
        {
            if format!("{:?}", e).contains("BucketAlreadyExists") {
                new_bucket_name += &env::var("NEW_BUCKET_SUFFIX").expect(
                    "NEW_BUCKET_SUFFIX must be set to avoid conflicts with existing buckets",
                );
                let _ = new_client
                    .create_bucket()
                    .bucket(&new_bucket_name)
                    .send()
                    .await;
            } else if !format!("{:?}", e).contains("BucketAlreadyOwnedByYou") {
                panic!("{:?}", e);
            }
        }

        println!("New Bucket: {}", new_bucket_name);

        let migrated_objects: HashSet<String> = new_client
            .list_objects_v2()
            .max_keys(MAX_KEYS)
            .bucket(&new_bucket_name)
            .send()
            .await
            .unwrap()
            .contents
            .unwrap_or(vec![])
            .iter()
            .map(|object| object.key.clone().unwrap())
            .collect();

        let mut objects = old_client
            .list_objects()
            .max_keys(MAX_KEYS)
            .bucket(bucket_name)
            .send()
            .await
            .unwrap()
            .contents
            .unwrap_or(vec![]);
        objects = objects
            .iter()
            .filter(|&object| !migrated_objects.contains(&object.key.clone().unwrap()))
            .cloned()
            .collect::<Vec<Object>>();

        let constraint = BucketLocationConstraint::from(
            env::var("NEW_AWS_REGION")
                .unwrap_or("us-east-1".to_string())
                .as_str(),
        );
        let bucket_config = CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();
        let _ = new_client
            .create_bucket()
            .create_bucket_configuration(bucket_config)
            .bucket(&new_bucket_name)
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

            if object.content_length().unwrap_or(0) < CHUNK_SIZE as i64 {
                singlepart_upload(&new_client, &new_bucket_name, object_key, object)
                    .await
                    .unwrap();
            } else {
                multipart_upload(&new_client, &new_bucket_name, object_key, object)
                    .await
                    .unwrap();
            }
        }
    }

    println!("Done!");
}

async fn singlepart_upload(
    client: &Client,
    bucket_name: &str,
    object_key: &str,
    object: GetObjectOutput,
) -> Result<(), aws_sdk_s3::Error> {
    client
        .put_object()
        .bucket(bucket_name)
        .key(object_key)
        .body(object.body)
        .send()
        .await?;
    Ok(())
}

async fn multipart_upload(
    client: &Client,
    bucket_name: &str,
    object_key: &str,
    mut object: GetObjectOutput,
) -> Result<()> {
    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(bucket_name)
        .key(object_key)
        .send()
        .await
        .unwrap();
    let upload_id = multipart_upload_res.upload_id.unwrap();

    let mut part = vec![];
    let mut part_number = 0;
    let mut upload_tasks = vec![];

    while let Some(bytes) = object.body.try_next().await.unwrap() {
        part.extend_from_slice(&bytes);
        if part.len() >= CHUNK_SIZE {
            part_number += 1;
            let body = take(&mut part);
            let client = client.clone();
            let bucket_name = bucket_name.to_string();
            let object_key = object_key.to_string();
            let upload_id = upload_id.clone();
            let body = body.to_vec();
            let task = tokio::spawn(async move {
                upload_part(
                    &client,
                    &bucket_name,
                    &object_key,
                    part_number,
                    &upload_id,
                    body,
                )
                .await
            });
            upload_tasks.push(task);
        }
    }

    if !part.is_empty() {
        part_number += 1;
        let client = client.clone();
        let bucket_name = bucket_name.to_string();
        let object_key = object_key.to_string();
        let upload_id = upload_id.clone();
        let task = tokio::spawn(async move {
            upload_part(
                &client,
                &bucket_name,
                &object_key,
                part_number,
                &upload_id,
                part,
            )
            .await
        });
        upload_tasks.push(task);
    }

    let completed_uploads = futures::future::try_join_all(upload_tasks)
        .await
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, res)| {
            CompletedPart::builder()
                .e_tag(res.as_ref().unwrap().e_tag().unwrap_or_default())
                .part_number(i as i32 + 1)
                .build()
        })
        .collect();

    client
        .complete_multipart_upload()
        .bucket(bucket_name)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_uploads))
                .build(),
        )
        .send()
        .await
        .unwrap();

    Ok(())
}

async fn upload_part(
    client: &Client,
    bucket_name: &str,
    object_key: &str,
    part_number: i32,
    upload_id: &str,
    body: Vec<u8>,
) -> Result<UploadPartOutput> {
    client
        .upload_part()
        .bucket(bucket_name)
        .key(object_key)
        .part_number(part_number)
        .upload_id(upload_id)
        .body(body.into())
        .send()
        .await
        .map_err(Into::into)
}
