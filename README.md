# s3copy

- あるS3リージョン内の全てのバケットを他のS3リージョンに移す
- S3、Minio、Wasabi等の相互変換も可能

## 使い方

1. `.old.credentials` と `.new.credentials` を用意（AWS Credentialsの書き方）
2. 環境変数 `(OLD|NEW)_AWS_REGION` と `(OLD|NEW)_AWS_ENDPOINT_URL` を `.env.local` に定義
3. `wasabi`等を使う際にバケット名の重複を回避したい場合には、`NEW_BUCKET_SUFFIX`を設定
4. `task` で実行
