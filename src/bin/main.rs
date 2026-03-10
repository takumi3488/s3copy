use std::{collections::HashSet, env, mem::take, time::Duration};

use anyhow::{Context, Result};
use aws_config::{
    retry::RetryConfig, stalled_stream_protection::StalledStreamProtectionConfig, Region,
};
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

const MAX_KEYS: i32 = 1_000_000;
const CHUNK_SIZE: usize = 5 * 1024 * 1024; // 5MB

async fn get_client(
    env_config_files: EnvConfigFiles,
    region: Region,
    endpoint_url: Option<&str>,
) -> Client {
    let mut config_loader = aws_config::from_env()
        .profile_files(env_config_files)
        .region(region)
        .retry_config(RetryConfig::standard().with_max_attempts(u32::MAX))
        .stalled_stream_protection(
            StalledStreamProtectionConfig::enabled()
                .grace_period(Duration::from_secs(60))
                .build(),
        );
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

async fn resolve_new_bucket_name(
    new_client: &Client,
    bucket_name: &str,
) -> Result<String> {
    let mut new_bucket_name = bucket_name.to_string();

    if let Err(e) = new_client
        .create_bucket()
        .bucket(&new_bucket_name)
        .send()
        .await
    {
        if format!("{e:?}").contains("BucketAlreadyExists") {
            new_bucket_name += &env::var("NEW_BUCKET_SUFFIX").context(
                "NEW_BUCKET_SUFFIX must be set to avoid conflicts with existing buckets",
            )?;
            let _ = new_client
                .create_bucket()
                .bucket(&new_bucket_name)
                .send()
                .await;
        } else if !format!("{e:?}").contains("BucketAlreadyOwnedByYou") {
            return Err(e.into());
        }
    }

    Ok(new_bucket_name)
}

async fn migrate_bucket(
    old_client: &Client,
    new_client: &Client,
    bucket_name: &str,
) -> Result<()> {
    println!("Bucket: {bucket_name}");

    let new_bucket_name = resolve_new_bucket_name(new_client, bucket_name).await?;
    println!("New Bucket: {new_bucket_name}");

    let migrated_objects: HashSet<String> = new_client
        .list_objects_v2()
        .max_keys(MAX_KEYS)
        .bucket(&new_bucket_name)
        .send()
        .await
        .context("Failed to list objects in new bucket")?
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|object| object.key)
        .collect();

    let mut objects = old_client
        .list_objects()
        .max_keys(MAX_KEYS)
        .bucket(bucket_name)
        .send()
        .await
        .context("Failed to list objects in old bucket")?
        .contents
        .unwrap_or_default();
    objects = objects
        .into_iter()
        .filter(|object| {
            object
                .key
                .as_deref()
                .is_some_and(|k| !migrated_objects.contains(k))
        })
        .collect::<Vec<Object>>();

    let constraint = BucketLocationConstraint::from(
        env::var("NEW_AWS_REGION")
            .unwrap_or_else(|_| "us-east-1".to_string())
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
        let object_key = object.key.as_deref().context("Object key is None")?;
        println!("Object: {object_key}");

        let object = old_client
            .get_object()
            .bucket(bucket_name)
            .key(object_key)
            .send()
            .await
            .context("Failed to get object")?;

        if object.content_length().unwrap_or(0) < 5 * 1024 * 1024 {
            singlepart_upload(new_client, &new_bucket_name, object_key, object)
                .await
                .context("Singlepart upload failed")?;
        } else {
            multipart_upload(new_client, &new_bucket_name, object_key, object)
                .await
                .context("Multipart upload failed")?;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let old_client = get_client(
        EnvConfigFiles::builder()
            .with_file(EnvConfigFileKind::Credentials, ".old.credentials")
            .build(),
        region_from_str(
            env::var("OLD_AWS_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string())
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
                .unwrap_or_else(|_| "us-east-1".to_string())
                .as_str(),
        ),
        env::var("NEW_AWS_ENDPOINT_URL").ok().as_deref(),
    )
    .await;

    let buckets = old_client
        .list_buckets()
        .send()
        .await
        .context("Failed to list buckets")?
        .buckets
        .unwrap_or_default();

    for bucket in buckets {
        let bucket_name = bucket.name.as_deref().unwrap_or_default();
        migrate_bucket(&old_client, &new_client, bucket_name).await?;
    }

    println!("Done!");
    Ok(())
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
    eprintln!("Starting multipart upload for: {object_key}");

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(bucket_name)
        .key(object_key)
        .send()
        .await
        .with_context(|| format!("Failed to create multipart upload for {object_key}"))?;

    let upload_id = multipart_upload_res
        .upload_id
        .with_context(|| format!("Missing upload_id for {object_key}"))?;

    let mut part = vec![];
    let mut part_number = 0;
    let mut upload_tasks = vec![];

    while let Some(bytes) = object
        .body
        .try_next()
        .await
        .with_context(|| format!("Failed to read object body stream for {object_key}"))?
    {
        part.extend_from_slice(&bytes);
        if part.len() >= CHUNK_SIZE {
            part_number += 1;
            let body = take(&mut part);
            let client = client.clone();
            let bucket_name = bucket_name.to_string();
            let object_key = object_key.to_string();
            let upload_id = upload_id.clone();
            let body = body.clone();
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

    let upload_results = futures::future::try_join_all(upload_tasks)
        .await
        .with_context(|| format!("Failed to join upload tasks for {object_key}"))?;

    let completed_uploads: Result<Vec<CompletedPart>> = upload_results
        .iter()
        .enumerate()
        .map(|(i, res)| {
            let e_tag = res
                .as_ref()
                .ok()
                .and_then(|r| r.e_tag())
                .unwrap_or_default();
            let part_num = i32::try_from(i).context("Part number overflow")? + 1;
            Ok(CompletedPart::builder()
                .e_tag(e_tag)
                .part_number(part_num)
                .build())
        })
        .collect();
    let completed_uploads = completed_uploads?;

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
        .with_context(|| format!("Failed to complete multipart upload for {object_key}"))?;

    eprintln!("Successfully completed multipart upload for: {object_key}");

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
