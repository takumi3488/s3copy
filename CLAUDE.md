# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## プロジェクト概要

S3バケットとオブジェクトを異なるリージョン間で移行するRustツール。AWS S3、MinIO、Wasabi等のS3互換ストレージ間での移行に対応。

## ビルドと実行コマンド

### 基本的なコマンド
```bash
# 開発用ビルドで移行実行
task
# または
cargo run

# リリースビルドで移行実行（本番推奨）
task run:release
# または
cargo run --release

# 削除ツールの実行
task delete
# または
cargo run --bin delete --release

# ビルドのみ
cargo build
cargo build --release
```

### 開発用コマンド
```bash
# リンター
cargo clippy

# フォーマッター
cargo fmt

# 依存関係の更新
cargo update
```

## アーキテクチャ

### バイナリ構成

プロジェクトには2つのバイナリが存在:
- **main.rs**: S3バケット/オブジェクトの移行ツール
- **delete.rs**: S3バケットとオブジェクトの削除ツール

### クライアント設定 (main.rs)

```rust
get_client(env_config_files, region, endpoint_url)
```

- **2つのクライアント**: `old_client`（移行元）と `new_client`（移行先）
- **認証情報の読み込み**: `.old.credentials` と `.new.credentials` から読み込み
- **リトライ設定**: `u32::MAX` まで自動リトライ
- **パススタイル**: `force_path_style(true)` でパスベースのアクセスに対応

### 環境変数（.env.localに設定）

```bash
OLD_AWS_REGION=us-east-1           # 移行元リージョン
NEW_AWS_REGION=ap-northeast-1      # 移行先リージョン
OLD_AWS_ENDPOINT_URL=              # オプション: カスタムエンドポイント（MinIO等）
NEW_AWS_ENDPOINT_URL=              # オプション: カスタムエンドポイント
NEW_BUCKET_SUFFIX=                 # オプション: バケット名重複時のサフィックス
```

### アップロード戦略

**ファイルサイズに基づく自動選択**:
- **5MB未満**: シングルパートアップロード (`singlepart_upload`)
- **5MB以上**: マルチパートアップロード (`multipart_upload`)

**マルチパートアップロードの仕組み** (main.rs:197-289):
1. `create_multipart_upload` でアップロードセッションを開始
2. オブジェクトを5MBチャンクに分割
3. 各チャンクを `tokio::spawn` で並列アップロード
4. `futures::future::try_join_all` で全タスクの完了を待機
5. `complete_multipart_upload` でアップロードを完了

### 重複チェックと進捗管理 (main.rs:111-137)

```rust
let migrated_objects: HashSet<String> = ...;
```

- 移行先バケットの既存オブジェクトをHashSetに格納
- 移行元のオブジェクトリストから既に移行済みのものを除外
- 中断しても途中から再開可能

### バケット作成時の重複ハンドリング (main.rs:89-107)

1. 同じ名前でバケット作成を試行
2. `BucketAlreadyExists` エラー時:
   - `NEW_BUCKET_SUFFIX` を追加して再試行
   - 環境変数が未設定の場合はpanicで終了
3. `BucketAlreadyOwnedByYou` エラー時:
   - 既に自分が所有しているので続行

### 定数とリミット

```rust
const MAX_KEYS: i32 = 1000000;           // 1バケットあたりの最大オブジェクト数
const CHUNK_SIZE: usize = 5 * 1024 * 1024; // マルチパートの5MBチャンク
```

**重要な制限事項**: 1バケットあたり100万オブジェクトまで対応。それ以上の場合はページネーション実装が必要。

### 削除ツール (delete.rs)

1. 全バケットを列挙
2. 各バケット内の全オブジェクトを列挙（ページネーション対応）
3. 全オブジェクトを削除
4. 空になったバケットを削除

## 認証情報の設定

AWS CLIの `~/.aws/credentials` と同じ形式で以下のファイルを作成:

```ini
# .old.credentials（移行元）
[default]
aws_access_key_id = YOUR_OLD_ACCESS_KEY
aws_secret_access_key = YOUR_OLD_SECRET_KEY

# .new.credentials（移行先）
[default]
aws_access_key_id = YOUR_NEW_ACCESS_KEY
aws_secret_access_key = YOUR_NEW_SECRET_KEY
```

これらのファイルは `.gitignore` に含まれており、コミットされない。

## 依存関係

主要なクレート:
- **aws-sdk-s3**: AWS S3 API
- **aws-config**: AWS設定管理
- **tokio**: 非同期ランタイム（fullフィーチャー有効）
- **futures**: 非同期ユーティリティ
- **anyhow**: エラーハンドリング

## サポートされているリージョン

`region_from_str` 関数で定義 (main.rs:38-45):
- us-east-1
- ap-northeast-1
- ap-northeast-3

新しいリージョンを追加する場合は、この関数にマッチ条件を追加する必要がある。
