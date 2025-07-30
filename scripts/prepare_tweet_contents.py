#!/usr/bin/env python3
"""
検証対象URLからtweet_idを抽出して重複排除したリストを作成するスクリプト

このスクリプトは、日本語ファクトチェックデータセットのCSVファイルから
検証対象URLを読み取り、TwitterのツイートIDを抽出して重複を除いた
リストを作成します。
"""

import argparse
import csv
import json
import os
import re
import time
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests


def extract_tweet_id_from_url(url: str) -> Optional[str]:
    """
    X（旧Twitter）のURLからtweet_idを抽出する

    Args:
        url (str): TwitterのURL

    Returns:
        Optional[str]: 抽出されたtweet_id、該当しない場合はNone
    """
    # Twitter URLのパターン
    # https://twitter.com/username/status/1234567890
    # https://x.com/username/status/1234567890

    patterns = [
        r"https?://(?:twitter\.com|x\.com)/\w+/status/(\d+)",
        r"https?://(?:twitter\.com|x\.com)/i/status/(\d+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)

    return None


def extract_tweet_ids_from_csv(
    csv_file: str, target_column: str = "検証対象URL"
) -> Set[str]:
    """
    CSVファイルから検証対象URLを読み取り、tweet_idを抽出する

    Args:
        csv_file (str): CSVファイルのパス
        target_column (str): 検証対象URLが含まれる列名

    Returns:
        Set[str]: 重複排除されたtweet_idのセット
    """
    tweet_ids = set()

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            url = row.get(target_column, "")
            if url:
                tweet_id = extract_tweet_id_from_url(url)
                if tweet_id:
                    tweet_ids.add(tweet_id)

    return tweet_ids


def save_tweet_ids(tweet_ids: Set[str], output_dir: str = "data") -> None:
    """
    tweet_idをCSV形式で保存する

    Args:
        tweet_ids (Set[str]): tweet_idのセット
        output_dir (str): 出力ディレクトリ
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # リストに変換してソート
    sorted_tweet_ids = sorted(list(tweet_ids))

    # CSVファイルとして保存
    csv_file = output_path / "tweet_ids.csv"
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["tweet_id"])
        for tweet_id in sorted_tweet_ids:
            writer.writerow([tweet_id])

    print(f"抽出されたtweet_id数: {len(sorted_tweet_ids)}")
    print("保存先:")
    print(f"  CSV: {csv_file}")


def chunk_list(lst: List[str], chunk_size: int = 100) -> List[List[str]]:
    """
    リストを指定されたサイズのチャンクに分割する

    Args:
        lst (List[str]): 分割対象のリスト
        chunk_size (int): チャンクサイズ（デフォルト: 100）

    Returns:
        List[List[str]]: 分割されたチャンクのリスト
    """
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


def get_posts_from_x_api(
    tweet_ids: List[str], bearer_token: str, output_dir: str = "data"
) -> Dict[str, any]:
    """
    X API v2を使用してツイートデータを取得する

    Args:
        tweet_ids (List[str]): 取得するtweet_idのリスト
        bearer_token (str): X API Bearer Token
        output_dir (str): 出力ディレクトリ

    Returns:
        Dict[str, any]: API応答データ
    """
    url = "https://api.x.com/2/tweets"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }

    # クエリパラメータ
    params = {
        "ids": ",".join(tweet_ids),
        "tweet.fields": "attachments,id,text,created_at",
        "expansions": "attachments.media_keys",
        "media.fields": "url,variants,preview_image_url",
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API呼び出しエラー: {e}")
        if hasattr(e.response, "text"):
            print(f"レスポンス: {e.response.text}")
        return {}


def download_media_file(url: str, filepath: Path) -> bool:
    """
    メディアファイルをダウンロードする

    Args:
        url (str): ダウンロードするメディアのURL
        filepath (Path): 保存先のファイルパス

    Returns:
        bool: ダウンロード成功時True、失敗時False
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()

        filepath.parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        return True
    except Exception as e:
        print(f"    メディアダウンロードエラー: {url} -> {e}")
        return False


def get_media_extension(media_type: str, url: str) -> str:
    """
    メディアタイプとURLから適切な拡張子を取得する

    Args:
        media_type (str): メディアタイプ (photo, video, animated_gif)
        url (str): メディアURL

    Returns:
        str: ファイル拡張子
    """
    if media_type == "photo":
        if ".jpg" in url or ".jpeg" in url:
            return ".jpg"
        elif ".png" in url:
            return ".png"
        elif ".webp" in url:
            return ".webp"
        else:
            return ".jpg"  # デフォルト
    elif media_type == "video":
        return ".mp4"
    elif media_type == "animated_gif":
        return ".gif"
    else:
        return ".bin"  # 不明な場合


def process_existing_tweets_data(
    json_file: str, output_dir: str = "data", save_individual: bool = True
) -> None:
    """
    既存のtweets_data.jsonからメディアファイルを取得する

    Args:
        json_file (str): tweets_data.jsonファイルのパス
        output_dir (str): 出力ディレクトリ
        save_individual (bool): 個別ツイートとして保存するかどうか
    """
    json_path = Path(json_file)
    if not json_path.exists():
        print(f"エラー: ファイル '{json_file}' が見つかりません")
        return

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            tweets_data = json.load(f)

        print(f"JSONファイルを読み込み中: {json_file}")

        if save_individual:
            print("\n個別ツイートデータを処理中...")
            total_tweets = 0
            total_media = 0

            for response_data in tweets_data:
                tweets = response_data.get("data", [])
                includes = response_data.get("includes", {})

                for tweet in tweets:
                    print(f"  ツイート処理中: {tweet.get('id')}")
                    media_count = save_individual_tweet(tweet, includes, output_dir)
                    total_tweets += 1
                    total_media += media_count

            print(
                f"処理完了: {total_tweets} 件のツイート, {total_media} 件のメディアファイルを処理しました"
            )
        else:
            print("個別保存が無効になっています")

    except json.JSONDecodeError as e:
        print(f"JSONファイルの読み込みエラー: {e}")
    except Exception as e:
        print(f"処理エラー: {e}")


def save_individual_tweet(
    tweet_data: Dict, includes_data: Dict, output_dir: str = "data"
) -> int:
    """
    個別のツイートデータを専用ディレクトリに保存する

    Args:
        tweet_data (Dict): ツイートデータ
        includes_data (Dict): API応答のincludesデータ
        output_dir (str): 出力ディレクトリ

    Returns:
        int: ダウンロードしたメディアファイル数
    """
    tweet_id = tweet_data.get("id")
    if not tweet_id:
        return 0

    # ツイート用ディレクトリを作成
    output_path = Path(output_dir) / "individual_tweets" / tweet_id
    output_path.mkdir(parents=True, exist_ok=True)

    # tweet.jsonを保存
    tweet_file = output_path / "tweet.json"
    with open(tweet_file, "w", encoding="utf-8") as f:
        json.dump(tweet_data, f, ensure_ascii=False, indent=2)

    # メディアファイルの処理
    downloaded_files = []
    media_data = includes_data.get("media", [])
    media_count = 0

    if media_data and "attachments" in tweet_data:
        media_keys = tweet_data["attachments"].get("media_keys", [])

        photo_count = 0
        video_count = 0
        gif_count = 0

        for media_key in media_keys:
            # media_keyに対応するメディア情報を検索
            media_info = None
            for media in media_data:
                if media.get("media_key") == media_key:
                    media_info = media
                    break

            if not media_info:
                continue

            media_type = media_info.get("type", "")

            # ファイル名とURLを決定
            if media_type == "photo":
                photo_count += 1
                filename = f"photo_{photo_count}"
                media_url = media_info.get("url", "")
            elif media_type == "video":
                video_count += 1
                filename = f"video_{video_count}"
                # 動画の場合は最高品質のvariantを選択
                variants = media_info.get("variants", [])
                media_url = ""
                max_bitrate = 0
                for variant in variants:
                    bitrate = variant.get("bit_rate", 0)
                    if bitrate > max_bitrate:
                        max_bitrate = bitrate
                        media_url = variant.get("url", "")
            elif media_type == "animated_gif":
                gif_count += 1
                filename = f"gif_{gif_count}"
                variants = media_info.get("variants", [])
                media_url = variants[0].get("url", "") if variants else ""
            else:
                continue

            if media_url:
                extension = get_media_extension(media_type, media_url)
                filepath = output_path / f"{filename}{extension}"

                print(f"    メディアダウンロード中: {filename}{extension}")
                if download_media_file(media_url, filepath):
                    downloaded_files.append(f"{filename}{extension}")
                    media_count += 1

    return media_count


def save_tweets_data(
    tweets_data: List[Dict], output_dir: str = "data", save_individual: bool = True
) -> None:
    """
    取得したツイートデータを保存する

    Args:
        tweets_data (List[Dict]): ツイートデータのリスト
        output_dir (str): 出力ディレクトリ
        save_individual (bool): 個別ツイートとして保存するかどうか
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # JSONファイルとして保存
    json_file = output_path / "tweets_data.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(tweets_data, f, ensure_ascii=False, indent=2)

    print("ツイートデータ保存先:")
    print(f"  JSON: {json_file}")

    # 個別ツイート保存
    if save_individual:
        print("\n個別ツイートデータを保存中...")
        total_tweets = 0
        total_media = 0
        for response_data in tweets_data:
            tweets = response_data.get("data", [])
            includes = response_data.get("includes", {})

            for tweet in tweets:
                media_count = save_individual_tweet(tweet, includes, output_dir)
                total_tweets += 1
                total_media += media_count

        print(
            f"個別保存完了: {total_tweets} 件のツイート, "
            f"{total_media} 件のメディアを処理しました"
        )
    """
    取得したツイートデータを保存する

    Args:
        tweets_data (List[Dict]): ツイートデータのリスト
        output_dir (str): 出力ディレクトリ
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # JSONファイルとして保存
    json_file = output_path / "tweets_data.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(tweets_data, f, ensure_ascii=False, indent=2)

    print(f"ツイートデータ保存先:")
    print(f"  JSON: {json_file}")


def fetch_all_tweets(
    tweet_ids: Set[str],
    bearer_token: str,
    output_dir: str = "data",
    is_free_plan: bool = True,
    save_individual: bool = True,
) -> None:
    """
    すべてのtweet_idに対してAPIを呼び出し、ツイートデータを取得する

    Args:
        tweet_ids (Set[str]): tweet_idのセット
        bearer_token (str): X API Bearer Token
        output_dir (str): 出力ディレクトリ
        is_free_plan (bool): Free planかどうか（デフォルト: True）
        save_individual (bool): 個別保存するかどうか（デフォルト: True）
    """
    sorted_tweet_ids = sorted(list(tweet_ids))
    chunks = chunk_list(sorted_tweet_ids, 100)

    plan_type = "paid" if not is_free_plan else "Free"
    wait_time = "60秒" if not is_free_plan else "15分"

    print(
        f"合計 {len(sorted_tweet_ids)} 件のツイートを {len(chunks)} 回のAPI呼び出しで取得します"
    )
    print(f"使用プラン: {plan_type} (リクエスト間隔: {wait_time})")

    all_tweets_data = []
    failed_chunks = []

    for i, chunk in enumerate(chunks, 1):
        print(f"進行状況: {i}/{len(chunks)} - {len(chunk)} 件のツイートを取得中...")

        try:
            tweets_data = get_posts_from_x_api(chunk, bearer_token, output_dir)

            if tweets_data and "data" in tweets_data:
                all_tweets_data.append(tweets_data)
                print(f"  成功: {len(tweets_data.get('data', []))} 件のツイートを取得")
            else:
                print(f"  警告: チャンク {i} でデータが取得できませんでした")
                failed_chunks.append((i, chunk))

            # API制限を考慮して待機
            if i < len(chunks):
                if is_free_plan:
                    # Free plan: 1回/15分 (900秒待機)
                    print(f"    Free plan制限のため15分間待機します...")
                    time.sleep(900)  # 15分 = 900秒
                else:
                    # Basic plan以上: 15回/15分 (60秒待機)
                    if len(chunks) > 15:
                        print(f"    次のリクエストまで60秒待機します...")
                        time.sleep(60)

        except Exception as e:
            print(f"  エラー: チャンク {i} の処理に失敗しました: {e}")
            failed_chunks.append((i, chunk))

    # 結果を保存
    if all_tweets_data:
        save_tweets_data(all_tweets_data, output_dir, save_individual)
        total_tweets = sum(len(data.get("data", [])) for data in all_tweets_data)
        print(f"\n取得完了: 合計 {total_tweets} 件のツイートを取得しました")

    # 失敗したチャンクがある場合は報告
    if failed_chunks:
        print(f"\n警告: {len(failed_chunks)} 個のチャンクで取得に失敗しました:")
        for chunk_num, chunk_ids in failed_chunks:
            print(f"  チャンク {chunk_num}: {len(chunk_ids)} 件")


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description="検証対象URLからtweet_idを抽出して重複排除したリストを作成"
    )
    parser.add_argument("input_csv", help="入力CSVファイルのパス")
    parser.add_argument(
        "--column",
        default="検証対象URL",
        help="検証対象URLが含まれる列名 (デフォルト: 検証対象URL)",
    )
    parser.add_argument(
        "--output-dir", default="data", help="出力ディレクトリ (デフォルト: data)"
    )
    parser.add_argument(
        "--fetch-tweets",
        action="store_true",
        help="X API v2を使用してツイートデータを取得する",
    )
    parser.add_argument(
        "--bearer-token",
        help="X API Bearer Token（環境変数 X_BEARER_TOKEN からも取得可能）",
    )
    parser.add_argument(
        "--paid-plan",
        action="store_true",
        help="X API有料プラン（Basic以上）を使用する場合に指定（デフォルト: Free）",
    )
    parser.add_argument(
        "--save-individual",
        action="store_true",
        default=True,
        help="tweet_idごとに個別ディレクトリに保存する（デフォルト: True）",
    )
    parser.add_argument(
        "--no-save-individual",
        action="store_true",
        help="個別ディレクトリへの保存を無効にする",
    )
    parser.add_argument(
        "--process-json",
        help="既存のtweets_data.jsonファイルからメディアを取得する",
    )

    args = parser.parse_args()

    # 既存JSONファイルの処理
    if args.process_json:
        print("既存のtweets_data.jsonからメディアファイルを取得します...")

        # 個別保存オプションを判定
        save_individual = args.save_individual and not args.no_save_individual

        process_existing_tweets_data(
            args.process_json, args.output_dir, save_individual
        )
        return 0

    # CSVファイルの存在確認
    if not Path(args.input_csv).exists():
        print(f"エラー: ファイル '{args.input_csv}' が見つかりません")
        return 1

    try:
        # tweet_idを抽出
        print(f"CSVファイルを読み込み中: {args.input_csv}")
        tweet_ids = extract_tweet_ids_from_csv(args.input_csv, args.column)

        if not tweet_ids:
            print("警告: tweet_idが見つかりませんでした")
            return 0

        # 結果を保存
        save_tweet_ids(tweet_ids, args.output_dir)

        # サンプルのtweet_idを表示
        print("\nサンプルtweet_id (最初の5件):")
        for i, tweet_id in enumerate(sorted(tweet_ids)):
            if i >= 5:
                break
            print(f"  {tweet_id}")

        # API取得機能が有効な場合
        if args.fetch_tweets:
            # Bearer Tokenを取得
            bearer_token = args.bearer_token or os.getenv("X_BEARER_TOKEN")

            if not bearer_token:
                print("\nエラー: Bearer Tokenが指定されていません")
                print(
                    "--bearer-token オプションまたは環境変数 X_BEARER_TOKEN を設定してください"
                )
                return 1

            # プランタイプを表示
            plan_type = "Paid" if args.paid_plan else "Free"
            print(f"\nX API v2を使用してツイートデータを取得します... ({plan_type})")

            # Free planかどうかを判定（--paid-planが指定されていない場合はTrue）
            is_free_plan = not args.paid_plan

            # 個別保存オプションを判定
            save_individual = args.save_individual and not args.no_save_individual

            fetch_all_tweets(
                tweet_ids, bearer_token, args.output_dir, is_free_plan, save_individual
            )

        return 0

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
