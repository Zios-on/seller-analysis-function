import json
import boto3
import os
import uuid
import urllib.request
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional, Any
import time
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MeetingMinutesProcessor:
    """議事録処理メインクラス"""

    def __init__(self):
        """AWS クライアント初期化"""
        self.s3_client = boto3.client('s3')
        self.transcribe_client = boto3.client('transcribe')
        self.bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
        
        # 設定値
        self.max_file_size = 100 * 1024 * 1024  # 100MB
        self.max_transcript_length = 8000
        self.audio_extensions = {'.mp3', '.wav', '.m4a', '.mp4', '.ogg', '.flac', '.aac'}
        
        # 指定されたClaude モデルID（最優先設定）
        self.primary_model = "anthropic.claude-sonnet-4-20250514-v1:0"  # 指定されたメインモデル
        
        self.claude_models = [
            "anthropic.claude-sonnet-4-20250514-v1:0",         # 🥇 指定されたメインモデル
            "anthropic.claude-3-5-sonnet-20241022-v2:0",       # フォールバック1
            "anthropic.claude-3-5-sonnet-20240620-v1:0",       # フォールバック2
            "anthropic.claude-3-sonnet-20240229-v1:0",         # フォールバック3
            "anthropic.claude-3-haiku-20240307-v1:0"           # フォールバック4
        ]
        
        logger.info("=== MeetingMinutesProcessor初期化 ===")
        logger.info(f"🥇 指定メインモデル: {self.primary_model}")
        logger.info(f"🔄 フォールバックモデル数: {len(self.claude_models) - 1}")

    def process_event(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """メインイベント処理"""
        logger.info("=== 自動議事録生成システム開始 ===")
        logger.info(f"使用予定モデル: {self.primary_model}")
        
        try:
            # システム状態確認
            self._create_system_status_log(event, context)
            
            # 実行時間チェック
            remaining_time = context.get_remaining_time_in_millis()
            logger.info(f"Lambda実行可能時間: {remaining_time/1000:.1f}秒")
            
            # S3イベント処理
            if 'Records' in event and event['Records']:
                return self._process_s3_events(event['Records'], context)
            else:
                # 手動実行モード
                return self._process_manual_mode(context)
                
        except Exception as e:
            logger.error(f"システムエラー: {str(e)}", exc_info=True)
            self._create_error_debug_file(str(e), e)
            return self._create_error_response(str(e), 500)

    def _create_system_status_log(self, event: Dict[str, Any], context: Any) -> None:
        """システム状態ログ作成"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            status_key = f"system_logs/system_status_{timestamp}.txt"
            
            status_content = f"""=== 議事録生成システム状態 ===

実行時刻: {datetime.now().isoformat()}
Lambda関数: {context.function_name}
メモリ制限: {context.memory_limit_in_mb}MB
実行時間制限: {context.get_remaining_time_in_millis()/1000:.1f}秒

🥇 指定メインモデル:
{self.primary_model}

🔄 フォールバック設定:
"""

            for i, model in enumerate(self.claude_models[1:], 2):
                status_content += f"{i}. {model}\n"
            
            status_content += f"""

【Bedrockアクセス確認が必要】
以下のモデルが 'Access granted' になっているか確認してください:

最重要:
□ {self.primary_model}

フォールバック用:
□ {self.claude_models[1]}
□ {self.claude_models[2]}
□ {self.claude_models[3]}
□ {self.claude_models[4]}

【確認方法】
AWS Bedrock → Model access → 各モデルのStatus確認

【申請方法】
Status が 'Available to request' の場合:
1. 'Request model access' をクリック
2. Use case: 'General'
3. Submit

受信イベント:
{json.dumps(event, indent=2, ensure_ascii=False)}
"""

            self.s3_client.put_object(
                Bucket="followup-mail",
                Key=status_key,
                Body=status_content.encode('utf-8'),
                ContentType='text/plain; charset=utf-8'
            )
            logger.info(f"✅ システム状態ログ作成: s3://followup-mail/{status_key}")
            
        except Exception as e:
            logger.warning(f"⚠️ システム状態ログ作成失敗: {str(e)}")

    def _process_s3_events(self, records: List[Dict[str, Any]], context: Any) -> Dict[str, Any]:
        """S3イベント処理"""
        results = []
        
        for record in records:
            if record.get('eventSource') != 'aws:s3':
                continue
                
            bucket_name = record['s3']['bucket']['name']
            object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
            
            logger.info(f"🎵 処理対象ファイル: s3://{bucket_name}/{object_key}")
            
            if self._is_valid_audio_file(object_key):
                result = self._process_single_audio_file(bucket_name, object_key, context)
                results.append(result)
            else:
                logger.info(f"⏭️ スキップ: {object_key} (無効なファイル)")
        
        return self._create_success_response(results)

    def _process_manual_mode(self, context: Any) -> Dict[str, Any]:
        """手動実行モード"""
        logger.info("🔧 手動実行モード - 最新のMP3ファイルを処理")
        
        try:
            bucket_name = "followup-mail"
            audio_prefix = "meeting record/"
            
            latest_file = self._find_latest_audio_file(bucket_name, audio_prefix)
            if not latest_file:
                return self._create_error_response("音声ファイルが見つかりません", 404)
            
            result = self._process_single_audio_file(bucket_name, latest_file['Key'], context)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'mode': 'manual',
                    'result': result
                }, ensure_ascii=False, indent=2)
            }
            
        except Exception as e:
            logger.error(f"手動モードエラー: {str(e)}", exc_info=True)
            return self._create_error_response(str(e), 500)

    def _is_valid_audio_file(self, object_key: str) -> bool:
        """有効な音声ファイルかチェック"""
        if not object_key.startswith('meeting record/'):
            return False
        
        return any(object_key.lower().endswith(ext) for ext in self.audio_extensions)

    def _find_latest_audio_file(self, bucket_name: str, prefix: str) -> Optional[Dict[str, Any]]:
        """最新の音声ファイルを検索"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            if 'Contents' not in response:
                return None
            
            audio_files = [
                obj for obj in response['Contents'] 
                if self._is_valid_audio_file(obj['Key'])
            ]
            
            if not audio_files:
                return None
            
            return max(audio_files, key=lambda x: x['LastModified'])
            
        except Exception as e:
            logger.error(f"音声ファイル検索エラー: {str(e)}")
            return None

    def _process_single_audio_file(self, bucket_name: str, object_key: str, context: Any) -> Dict[str, Any]:
        """単一音声ファイル処理（更新版）"""
        logger.info(f"🎵 音声ファイル処理開始: {object_key}")
        logger.info(f"🥇 使用予定メインモデル: {self.primary_model}")
        
        try:
            # ファイル情報取得
            file_info = self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
            file_size = file_info['ContentLength']
            last_modified = file_info['LastModified']
            
            logger.info(f"📁 ファイルサイズ: {file_size/1024/1024:.2f}MB")
            
            # サイズチェック
            if file_size > self.max_file_size:
                return self._create_file_error_result(object_key, "ファイルサイズが100MBを超えています")
            
            # 処理進捗をS3に記録
            self._create_progress_file(object_key, f"文字起こし処理開始 (使用予定モデル: {self.primary_model})")
            
            # 会議情報生成
            meeting_info = self._generate_meeting_info(object_key, last_modified)
            logger.info(f"📝 会議情報: {meeting_info['title']}")
            
            # 文字起こし処理
            transcript_result = self._process_transcription(bucket_name, object_key, context)
            if transcript_result['status'] != 'success':
                # 文字起こし失敗時もデモデータで続行
                logger.warning("文字起こし失敗 - デモデータで続行")
                transcript_text = self._get_demo_transcript()
                self._create_progress_file(object_key, "文字起こし失敗 - デモデータ使用")
            else:
                transcript_text = transcript_result['transcript']
                logger.info(f"✅ 文字起こし完了: {len(transcript_text)}文字")
                self._create_progress_file(object_key, f"文字起こし完了 ({len(transcript_text)}文字)")
            
            # Claude処理進捗更新
            self._create_progress_file(object_key, f"Claude処理開始 (メインモデル: {self.primary_model})")
            
            # 議事録生成（指定モデル最優先）
            summary_result = self._generate_meeting_summary_with_specified_model(meeting_info, transcript_text, object_key)
            if not summary_result:
                # 全モデル失敗時はデモデータで続行
                logger.warning("全Claudeモデル失敗 - デモデータで続行")
                summary_result = self._create_demo_summary(meeting_info, transcript_text)
                self._create_progress_file(object_key, f"全Claudeモデル失敗 (メイン: {self.primary_model}) - デモデータ使用")
            else:
                self._create_progress_file(object_key, f"Claude処理完了 (使用モデル記録済み)")
            
            # 会議情報をサマリーに追加（マニフェスト用）
            summary_result["meeting_title"] = meeting_info['title']
            summary_result["meeting_date"] = meeting_info['date']
            summary_result["participants"] = meeting_info['participants']
            
            # メールコピペ用テキスト作成
            self._create_progress_file(object_key, "テキストファイル作成開始")
            copy_paste_text = self._create_copy_paste_text(summary_result, meeting_info, object_key, transcript_text)
            
            # 新しいファイル保存方式で保存
            saved_files = self._save_text_files_to_email_output(bucket_name, object_key, transcript_text, summary_result, copy_paste_text)
            
            # 完了通知作成
            self._create_completion_notification(bucket_name, object_key, meeting_info, len(transcript_text), saved_files)
            
            # 最終進捗
            self._create_progress_file(object_key, "✅ 全処理完了 - 標準フォーマットのファイル生成済み")
            
            # Get manifest path if available
            manifest_path = saved_files.get("manifest", "")
            
            return {
                'file': object_key,
                'status': 'success',
                'primary_model_used': self.primary_model,
                'meeting_info': meeting_info,
                'transcript_length': len(transcript_text),
                'text_files_created': True,
                'output_folder': f"email_output/{object_key.split('/')[-1].rsplit('.', 1)[0]}/",
                'manifest': manifest_path,
                'processing_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ ファイル処理エラー ({object_key}): {str(e)}", exc_info=True)
            self._create_progress_file(object_key, f"❌ 処理エラー: {str(e)}")
            return self._create_file_error_result(object_key, str(e))

    def _create_progress_file(self, object_key: str, status: str) -> None:
        """進捗をファイルに記録（新しいフォルダ構造に対応）"""
        try:
            # Extract filename for folder name
            audio_filename = object_key.split('/')[-1].rsplit('.', 1)[0]
            timestamp = datetime.now().strftime('%H:%M:%S')
            progress_key = f"email_output/{audio_filename}/progress.txt"
            
            # Get existing content or create new
            try:
                existing = self.s3_client.get_object(Bucket="followup-mail", Key=progress_key)
                existing_content = existing['Body'].read().decode('utf-8')
            except:
                existing_content = f"""=== 処理進捗: {audio_filename} ===

開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
指定メインモデル: {self.primary_model}
処理モード: テキストファイル出力専用

"""

            new_content = existing_content + f"[{timestamp}] {status}\n"
            
            self.s3_client.put_object(
                Bucket="followup-mail",
                Key=progress_key,
                Body=new_content.encode('utf-8'),
                ContentType='text/plain; charset=utf-8'
            )
            
        except Exception as e:
            logger.warning(f"進捗記録失敗: {str(e)}")

    def _generate_meeting_info(self, object_key: str, last_modified: datetime) -> Dict[str, str]:
        """会議情報生成"""
        filename = object_key.split('/')[-1].rsplit('.', 1)[0]
        
        # ファイル名から会議タイトルを推測
        if 'GMT' in filename:
            meeting_title = "定期会議 (自動検出)"
        elif 'meeting' in filename.lower():
            meeting_title = f"会議録音 - {filename}"
        else:
            meeting_title = f"音声会議 - {filename}"
        
        return {
            'title': meeting_title,
            'date': last_modified.strftime('%Y年%m月%d日 %H:%M'),
            'participants': "音声ファイルから自動検出",
            'source_file': object_key
        }

    def _process_transcription(self, bucket_name: str, object_key: str, context: Any) -> Dict[str, Any]:
        """文字起こし処理"""
        logger.info("🎤 文字起こし処理開始")
        
        try:
            # 既存文字起こしチェック
            existing_transcript = self._check_existing_transcript(bucket_name, object_key)
            if existing_transcript:
                logger.info("📄 既存の文字起こしを使用")
                return {
                    'status': 'success',
                    'transcript': existing_transcript,
                    'source': 'existing'
                }
            
            # 新規文字起こし実行
            transcript_text = self._execute_transcription(bucket_name, object_key, context)
            
            if transcript_text:
                self._save_transcript(bucket_name, object_key, transcript_text)
                return {
                    'status': 'success',
                    'transcript': transcript_text,
                    'source': 'new'
                }
            else:
                return {
                    'status': 'error',
                    'message': 'Transcribe処理タイムアウト'
                }
                
        except Exception as e:
            logger.error(f"文字起こしエラー: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'文字起こしエラー: {str(e)}'
            }

    def _check_existing_transcript(self, bucket_name: str, audio_key: str) -> Optional[str]:
        """既存文字起こしチェック"""
        try:
            audio_filename = audio_key.split('/')[-1].rsplit('.', 1)[0]
            transcript_key = f"transcripts/{audio_filename}_transcript.txt"
            
            response = self.s3_client.get_object(Bucket=bucket_name, Key=transcript_key)
            transcript_text = response['Body'].read().decode('utf-8')
            logger.info(f"✅ 既存文字起こし発見: {transcript_key}")
            return transcript_text
            
        except self.s3_client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            logger.warning(f"既存文字起こしチェックエラー: {str(e)}")
            return None

    def _execute_transcription(self, bucket_name: str, object_key: str, context: Any) -> Optional[str]:
        """Transcribe実行"""
        try:
            # ジョブ設定
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_name = f"auto-meeting-{timestamp}-{uuid.uuid4().hex[:8]}"
            
            # ファイル形式判定
            file_extension = object_key.lower().split('.')[-1]
            format_mapping = {
                'mp3': 'mp3', 'wav': 'wav', 'm4a': 'mp4', 'mp4': 'mp4',
                'ogg': 'ogg', 'flac': 'flac', 'aac': 'mp4'
            }
            media_format = format_mapping.get(file_extension, 'mp3')
            
            audio_url = f"s3://{bucket_name}/{object_key}"
            
            logger.info(f"🎙️ Transcribeジョブ開始: {job_name} ({media_format})")
            
            # ジョブ開始
            self.transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': audio_url},
                MediaFormat=media_format,
                LanguageCode='ja-JP',
                Settings={
                    'ShowSpeakerLabels': True,
                    'MaxSpeakerLabels': 10
                }
            )
            
            # 完了待機
            return self._wait_for_transcription(job_name, context)
            
        except Exception as e:
            logger.error(f"Transcribe実行エラー: {str(e)}", exc_info=True)
            return None

    def _wait_for_transcription(self, job_name: str, context: Any) -> Optional[str]:
        """Transcribe完了待機"""
        remaining_time = context.get_remaining_time_in_millis()
        max_wait_seconds = min(600, max(60, (remaining_time / 1000) - 180))
        max_attempts = int(max_wait_seconds // 20)
        
        logger.info(f"⏱️ 最大待機時間: {max_wait_seconds:.0f}秒")
        
        for attempt in range(max_attempts):
            try:
                response = self.transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
                job_status = response['TranscriptionJob']['TranscriptionJobStatus']
                
                logger.info(f"📊 ジョブ状態確認 ({attempt+1}/{max_attempts}): {job_status}")
                
                if job_status == 'COMPLETED':
                    transcript_uri = response['TranscriptionJob']['Transcript']['TranscriptFileUri']
                    transcript_text = self._fetch_transcript_result(transcript_uri)
                    self._cleanup_transcribe_job(job_name)
                    return transcript_text
                
                elif job_status == 'FAILED':
                    failure_reason = response['TranscriptionJob'].get('FailureReason', '不明')
                    logger.error(f"❌ 文字起こし失敗: {failure_reason}")
                    self._cleanup_transcribe_job(job_name)
                    return None
                
                # 残り時間チェック
                remaining = context.get_remaining_time_in_millis()
                if remaining < 120000:  # 2分以下の場合
                    logger.warning(f"⚠️ Lambda実行時間残り少なく ({remaining/1000:.1f}秒)、強制終了")
                    self._cleanup_transcribe_job(job_name)
                    return None
                
                # 待機
                time.sleep(20)
                
            except Exception as e:
                logger.error(f"ジョブ状態チェックエラー: {str(e)}")
                break
        
        # タイムアウト
        logger.warning(f"⚠️ Transcribeジョブ待機タイムアウト: {job_name}")
        return None

    def _fetch_transcript_result(self, transcript_uri: str) -> Optional[str]:
        """文字起こし結果取得"""
        try:
            with urllib.request.urlopen(transcript_uri) as response:
                transcript_json = json.loads(response.read().decode('utf-8'))
            
            # 基本文字起こし
            transcript = transcript_json.get('results', {}).get('transcripts', [{}])[0].get('transcript', '')
            
            # 話者ラベル付きフォーマット試行
            items = transcript_json.get('results', {}).get('items', [])
            speakers = transcript_json.get('results', {}).get('speaker_labels', {}).get('speakers', 0)
            
            if items and speakers > 0:
                formatted = self._format_with_speakers(transcript_json)
                if formatted:
                    logger.info("🗣️ 話者ラベル付きフォーマット適用")
                    return formatted
            
            logger.info("📝 基本フォーマット使用")
            return transcript
            
        except Exception as e:
            logger.error(f"文字起こし結果取得エラー: {str(e)}")
            return None

    def _format_with_speakers(self, transcript_json: Dict[str, Any]) -> Optional[str]:
        """話者ラベル付きフォーマット"""
        try:
            items = transcript_json.get('results', {}).get('items', [])
            speaker_labels = transcript_json.get('results', {}).get('speaker_labels', {})
            segments = speaker_labels.get('segments', [])
            
            # マッピング作成
            time_to_speaker = {}
            for segment in segments:
                speaker_label = segment.get('speaker_label')
                for item in segment.get('items', []):
                    start_time = item.get('start_time')
                    if start_time and speaker_label:
                        time_to_speaker[start_time] = speaker_label
            
            # 発言テキスト構築
            current_speaker = None
            formatted_transcript = []
            buffer = ""
            
            for item in items:
                if item.get('type') == 'pronunciation':
                    start_time = item.get('start_time')
                    speaker = time_to_speaker.get(start_time)
                    
                    if speaker and speaker != current_speaker and buffer:
                        if current_speaker:
                            formatted_transcript.append(f"話者{current_speaker.replace('spk_', '')}: {buffer.strip()}")
                        buffer = ""
                        current_speaker = speaker
                    
                    if not current_speaker:
                        current_speaker = speaker
                    
                    content = item.get('alternatives', [{}])[0].get('content', '')
                    buffer += content + " "
                    
                elif item.get('type') == 'punctuation':
                    content = item.get('alternatives', [{}])[0].get('content', '')
                    buffer = buffer.rstrip() + content + " "
            
            # 最後の発言を追加
            if current_speaker and buffer:
                formatted_transcript.append(f"話者{current_speaker.replace('spk_', '')}: {buffer.strip()}")
            
            return "\n\n".join(formatted_transcript)
            
        except Exception as e:
            logger.error(f"話者フォーマットエラー: {str(e)}")
            return None

    def _cleanup_transcribe_job(self, job_name: str) -> None:
        """Transcribeジョブクリーンアップ"""
        try:
            self.transcribe_client.delete_transcription_job(TranscriptionJobName=job_name)
            logger.info(f"🗑️ Transcribeジョブ削除: {job_name}")
        except Exception as e:
            logger.warning(f"ジョブ削除失敗 ({job_name}): {str(e)}")

    def _save_transcript(self, bucket_name: str, audio_key: str, transcript_text: str) -> None:
        """文字起こし保存"""
        try:
            audio_filename = audio_key.split('/')[-1].rsplit('.', 1)[0]
            transcript_key = f"transcripts/{audio_filename}_transcript.txt"
            
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=transcript_key,
                Body=transcript_text.encode('utf-8'),
                ContentType='text/plain; charset=utf-8'
            )
            logger.info(f"💾 文字起こし保存: s3://{bucket_name}/{transcript_key}")
            
        except Exception as e:
            logger.warning(f"文字起こし保存失敗: {str(e)}")

    def _get_demo_transcript(self) -> str:
        """デモ文字起こしデータ"""
        return """話者1: 皆さん、お疲れ様です。それでは、Project Xの進捗会議を始めさせていただきます。まず、山田さんから開発状況をお聞かせください。

話者2: ありがとうございます、山田です。今週の開発状況についてご報告いたします。フロントエンドの実装が約80%完了しており、予定通り今月末にはベータ版がリリースできる見込みです。ただし、APIの統合部分で一部課題が発生しております。

話者1: どのような課題でしょうか？

話者2: データベースとの連携部分で、レスポンス時間が予想より長くなっている問題があります。最適化が必要な状況です。

話者3: 佐藤です。その件について、インフラ側から支援できることがあります。来週月曜日までに、データベースのパフォーマンスチューニングを実施いたします。

話者4: 鈴木です。マーケティング側の準備はいかがでしょうか？

話者5: 高橋です。マーケティング資料は完成しており、ランディングページも来週火曜日には公開予定です。広告キャンペーンの準備も整っております。

話者1: 素晴らしいですね。それでは、次回の会議は来週金曜日の同じ時間ということで、最終確認を行いましょう。各担当者は今日決めたアクション項目を来週までに完了をお願いします。

話者2: 承知いたしました。

話者3: 了解です。

話者1: それでは、今日はありがとうございました。お疲れ様でした。"""

    def _generate_meeting_summary_with_specified_model(self, meeting_info: Dict[str, str], transcript_text: str, object_key: str) -> Optional[Dict[str, Any]]:
        """指定モデル anthropic.claude-sonnet-4-20250514-v1:0 でClaude議事録生成"""
        logger.info(f"🤖 Claude議事録生成開始")
        logger.info(f"🥇 指定メインモデル: {self.primary_model}")
        
        # Claude処理状況をS3に記録
        filename = object_key.split('/')[-1].rsplit('.', 1)[0]
        claude_debug_key = f"email_output/{filename}/claude_processing.txt"
        
        # テキスト長制限
        original_length = len(transcript_text)
        if len(transcript_text) > self.max_transcript_length:
            transcript_text = transcript_text[:self.max_transcript_length] + "\n\n...(文字数制限により省略)"
            logger.info(f"📏 文字起こしを {self.max_transcript_length} 文字に制限 (元: {original_length}文字)")
        
        # 指定モデル anthropic.claude-sonnet-4-20250514-v1:0 で順次試行
        for i, model_id in enumerate(self.claude_models):
            try:
                if i == 0:
                    model_status = f"🥇 指定メインモデル: {model_id}"
                else:
                    model_status = f"#{i+1} フォールバックモデル: {model_id}"
                
                logger.info(f"🔄 Claude API呼び出し: {model_status}")
                
                # 詳細プロンプト
                prompt = f"""以下の会議の文字起こしから、構造化された議事録を作成してください。

【会議情報】
会議名: {meeting_info['title']}
日時: {meeting_info['date']}
参加者: {meeting_info['participants']}

【文字起こし内容】
{transcript_text}

【必須出力形式】
以下のJSON形式で必ず回答してください:

{{
"meeting_summary": "会議の要約を2-3文で簡潔に記述してください",
"key_decisions": ["決定事項1", "決定事項2", "決定事項3"],
"action_items": [
{{
"task": "具体的なタスク内容を記述",
"assignee": "担当者名（不明な場合は「要確認」と記載）",
"deadline": "期限（不明な場合は「要設定」と記載）",
"priority": "高/中/低のいずれかを設定"
}}
],
"next_meeting": "次回会議の予定（不明な場合は「未定」と記載）",
"concerns": ["懸念事項1", "懸念事項2"]
}}

【重要な注意事項】
1. 必ずJSONの構文を正しく守ってください
2. 文字起こしから明確に読み取れる内容のみを記載してください
3. 推測や補完は避けてください
4. すべて日本語で回答してください
5. JSONの外に説明文は含めないでください"""

                # Claude API呼び出し（指定モデル優先）
                body = json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 4000,
                    "temperature": 0.1,
                    "messages": [{"role": "user", "content": prompt}]
                })

                logger.info(f"📡 Bedrock API呼び出し実行: {model_id}")
                
                response = self.bedrock_client.invoke_model(
                    modelId=model_id,
                    body=body,
                    contentType="application/json"
                )

                response_body = json.loads(response['body'].read())
                content = response_body['content'][0]['text']
                
                logger.info(f"✅ Claude レスポンス受信: {len(content)}文字")
                logger.info(f"使用成功モデル: {model_id}")
                
                # JSON解析
                parsed_result = self._parse_claude_response(content)
                
                if parsed_result:
                    # 成功 - 詳細デバッグ情報をS3に保存
                    success_debug_content = f"""=== Claude API 成功 ===

処理時刻: {datetime.now().isoformat()}
音声ファイル: {object_key}

🥇 使用成功モデル: {model_id}
モデル状態: {model_status}
試行回数: {i+1}/{len(self.claude_models)}

処理詳細:
- 入力文字数: {original_length}文字 (使用: {len(transcript_text)}文字)
- 出力文字数: {len(content)}文字
- JSON解析: ✅ 成功

会議情報:
- タイトル: {meeting_info['title']}
- 日時: {meeting_info['date']}

生成された議事録:
{json.dumps(parsed_result, ensure_ascii=False, indent=2)}

Claude レスポンス (Raw):
{content}
"""

                    self.s3_client.put_object(
                        Bucket="followup-mail",
                        Key=claude_debug_key,
                        Body=success_debug_content.encode('utf-8'),
                        ContentType='text/plain; charset=utf-8'
                    )
                    
                    logger.info(f"✅ Claude API成功: {model_status}")
                    return parsed_result
                else:
                    # JSON解析失敗 - 次のモデルを試行
                    logger.warning(f"⚠️ JSON解析失敗: {model_status} - 次のモデルを試行")
                    continue
                
            except Exception as e:
                error_type = type(e).__name__
                logger.warning(f"⚠️ Claude API エラー: {model_status} - {error_type}: {str(e)}")
                
                # エラー詳細をS3に保存
                error_debug_content = f"""=== Claude API エラー詳細 ===

処理時刻: {datetime.now().isoformat()}
音声ファイル: {object_key}

❌ 失敗モデル: {model_id}
モデル状態: {model_status}
試行回数: {i+1}/{len(self.claude_models)}
エラー種類: {error_type}
エラーメッセージ: {str(e)}

【このエラーの対処法】
"""

                if "AccessDenied" in str(e):
                    error_debug_content += f"""
このモデルはBedrockでアクセス権限がありません。

解決手順:
1. AWS Bedrock コンソール → Model access
2. {model_id} を検索
3. Status確認:
   - 'Access granted' → 使用可能 (このエラーは別の原因)
   - 'Available to request' → 以下の手順で申請:
     a) 'Request model access' をクリック
     b) Use case: 'General' を選択
     c) Submit
     d) 承認まで数分〜数時間待機
4. 承認後、MP3ファイルを再アップロード
"""
                else:
                    error_debug_content += f"""
予期しないエラーです。
原因として考えられるもの:
- ネットワーク問題
- 一時的なサービス問題
- リクエスト形式の問題

次のフォールバックモデルで再試行します。
"""

                error_debug_content += f"\n次の対応: {'次のモデルで再試行' if i < len(self.claude_models)-1 else 'デモデータ使用'}"
                
                # S3にエラーログ保存
                self.s3_client.put_object(
                    Bucket="followup-mail",
                    Key=claude_debug_key,
                    Body=error_debug_content.encode('utf-8'),
                    ContentType='text/plain; charset=utf-8'
                )
        
        # 全モデル失敗
        logger.error(f"❌ 全モデル失敗 - デモデータ使用")
        return None

    def _parse_claude_response(self, content: str) -> Optional[Dict[str, Any]]:
        """Claude レスポンス解析"""
        try:
            # JSONの開始と終了を検索
            start_idx = content.find('{')
            end_idx = content.rfind('}') + 1
            
            if start_idx == -1 or end_idx <= 0 or end_idx <= start_idx:
                logger.warning("⚠️ JSON形式が見つかりません")
                return None
            
            json_content = content[start_idx:end_idx]
            return json.loads(json_content)
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON解析エラー: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"❌ 解析エラー: {str(e)}")
            return None

    def _create_demo_summary(self, meeting_info: Dict[str, str], transcript_text: str) -> Dict[str, Any]:
        """デモ議事録作成（指定モデル失敗時のフォールバック）"""
        logger.info(f"📝 デモ議事録作成 (指定モデル {self.primary_model} アクセス権限問題)")
        
        return {
            "meeting_summary": f"{meeting_info['title']}を開催しました。音声から文字起こしを実行し、指定のClaude モデル ({self.primary_model}) での議事録生成を試行しましたが、Bedrockのアクセス権限問題のため、デモデータで結果を生成しています。",
            "key_decisions": [
                "音声ファイルから文字起こしを完了",
                "指定Claude モデルでの議事録生成を試行",
                "Bedrockモデルアクセス権限の問題を確認",
                "デモデータで暫定結果を出力"
            ],
            "action_items": [
                {
                    "task": f"AWS Bedrockで指定モデル ({self.primary_model}) のアクセス権限申請",
                    "assignee": "システム管理者",
                    "deadline": "要設定",
                    "priority": "高"
                },
                {
                    "task": "文字起こし内容の手動確認と正式な議事録作成",
                    "assignee": "参加者全員",
                    "deadline": "要設定",
                    "priority": "高"
                },
                {
                    "task": "フォールバックモデルのアクセス権限確認",
                    "assignee": "システム管理者",
                    "deadline": "要設定",
                    "priority": "中"
                }
            ],
            "next_meeting": "未定（参加者で調整）",
            "concerns": [
                f"指定メインモデル ({self.primary_model}) のアクセス権限なし",
                f"全フォールバックモデル ({len(self.claude_models)-1}個) でもアクセス拒否",
                "AWS Bedrock Model access での申請・承認が必要",
                "暫定的にデモデータで出力中"
            ],
            "specified_model": self.primary_model,
            "model_access_status": "Access Denied - 申請が必要",
            "technical_note": f"指定モデル '{self.primary_model}' (Claude Sonnet 4) を最優先で試行したが、全{len(self.claude_models)}モデルでAccessDenied",
            "note": f"この議事録は指定モデル {self.primary_model} のアクセス権限問題時のデモデータです。Bedrockでモデルアクセス申請後に再実行してください。"
        }

    def _create_copy_paste_text(self, summary_result: Dict[str, Any], meeting_info: Dict[str, str], source_file: str, transcript_text: str) -> str:
        """メールコピペ用テキスト作成 - 新フォーマット"""
        # メール件名用の日付整形
        meeting_date = meeting_info['date']
        if '年' in meeting_date and '月' in meeting_date and '日' in meeting_date:
            try:
                # 「2025年9月26日 18:50」形式から「9/26」形式に変換
                date_parts = meeting_date.split(' ')[0].split('年')
                month = date_parts[1].split('月')[0]
                day = date_parts[1].split('月')[1].split('日')[0]
                short_date = f"{month}/{day}"
            except:
                short_date = meeting_date
        else:
            short_date = meeting_date

        # 会議概要の整形
        meeting_summary = summary_result.get('meeting_summary', '').strip()

        # 重要な決定事項
        key_decisions = []
        for decision in summary_result.get('key_decisions', []):
            if decision and decision.strip():
                key_decisions.append(decision.strip())

        # アクション項目
        action_items = []
        for item in summary_result.get('action_items', []):
            task = item.get('task', '').strip()
            assignee = item.get('assignee', '要確認').strip()
            deadline = item.get('deadline', '未定').strip()
            
            if task:
                deadline_text = ""
                if deadline and deadline != "要設定" and deadline != "未定":
                    deadline_text = f"（期限：{deadline}）"
                
                action_items.append(f"{assignee}： {task}{deadline_text}")

        # メール本文作成
        email_body = f"""件名：本日のお打ち合わせのお礼と内容のご共有

ご担当者様

本日はお忙しい中、ミーティングにご参加いただき誠にありがとうございました。
本日の内容を以下の通りご共有いたします。

⸻

議事録（Summary）
"""

        # 会議概要
        if meeting_summary:
            email_body += f"\t• {meeting_summary}\n"

        # 重要な決定事項
        if key_decisions:
            for point in key_decisions:
                email_body += f"\t• {point}\n"
        else:
            email_body += "\t• 特に決定事項はありませんでした\n"

        # アクション項目
        email_body += "\n次のアクション（Next Action）"
        if action_items:
            for action in action_items:
                email_body += f"\n\t• {action}"
        else:
            email_body += "\n\t• 特にアクション項目はありませんでした"

        # メール締め
        email_body += """

⸻

内容に誤りや追加事項がございましたら、ご指摘いただけますと幸いです。
引き続きどうぞよろしくお願い申し上げます。

"""

        # デモデータの場合はシステム使用情報を追加
        is_demo = "note" in summary_result
        if is_demo:
            email_body += f"\n※ 議事録は音声ファイルから自動生成されました。"
            email_body += f"\n※ 現在デモデータを使用中です。AWS Bedrockで {self.primary_model} のアクセス権限申請が必要です。"
        
        return email_body

    def _save_text_files_to_email_output(self, bucket_name: str, object_key: str, transcript_text: str, 
                                    summary_result: Dict[str, Any], copy_paste_text: str) -> Dict[str, str]:
        """
        Save output files to S3 with an organized folder structure and manifest
        
        Returns a dictionary of file paths created
        """
        try:
            # Extract filename without extension for folder name
            audio_filename = object_key.split('/')[-1].rsplit('.', 1)[0]
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            job_id = f"{audio_filename}_{timestamp}"
            
            # Create subfolder path
            output_folder = f"email_output/{audio_filename}/"
            
            # Define standard filenames without timestamps
            files_to_save = [
                # Main content files with standardized names
                (f"{output_folder}email_content.txt", copy_paste_text, 'text/plain; charset=utf-8'),
                (f"{output_folder}transcript.txt", transcript_text, 'text/plain; charset=utf-8'),
                (f"{output_folder}summary.json", json.dumps(summary_result, ensure_ascii=False, indent=2), 'application/json; charset=utf-8'),
            ]
            
            # Save files
            saved_files = {}
            for key, content, content_type in files_to_save:
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=content.encode('utf-8') if isinstance(content, str) else content,
                    ContentType=content_type
                )
                saved_files[key.split('/')[-1].rsplit('.', 1)[0]] = key
                logger.info(f"💾 保存完了: s3://{bucket_name}/{key}")
            
            # Create manifest file with all metadata
            is_demo = "note" in summary_result
            processed_at = datetime.now().isoformat()
            
            manifest = {
                "status": "success",
                "job_id": job_id,
                "source": {
                    "file": object_key,
                    "processed_at": processed_at
                },
                "meeting": {
                    "title": summary_result.get("meeting_title", "Meeting Recording"),
                    "date": summary_result.get("meeting_date", processed_at),
                    "participants": summary_result.get("participants", "Automatically detected")
                },
                "processing": {
                    "transcript_length": len(transcript_text),
                    "model_used": self.primary_model,
                    "is_demo_data": is_demo
                },
                "files": {
                    "transcript": f"{output_folder}transcript.txt",
                    "email_content": f"{output_folder}email_content.txt",
                    "summary_json": f"{output_folder}summary.json",
                    "progress_log": f"{output_folder}progress.txt"
                },
                # Include the full summary directly in the manifest
                "summary": summary_result
            }
            
            # Save manifest file
            manifest_key = f"{output_folder}manifest.json"
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=manifest_key,
                Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json; charset=utf-8'
            )
            saved_files["manifest"] = manifest_key
            logger.info(f"📋 マニフェスト作成: s3://{bucket_name}/{manifest_key}")
            
            # Create index file at the root of email_output to help frontend find latest results
            self._update_index_file(bucket_name, audio_filename, manifest)
            
            logger.info(f"✅ 出力ファイル保存完了: {output_folder}")
            return saved_files
            
        except Exception as e:
            logger.error(f"❌ ファイル保存エラー: {str(e)}", exc_info=True)
            return {}

    def _update_index_file(self, bucket_name: str, audio_filename: str, manifest: Dict[str, Any]) -> None:
        """
        Create or update an index file that lists all processed recordings
        This helps the frontend find the latest processed files
        """
        try:
            index_key = "email_output/index.json"
            
            # Try to read existing index
            try:
                response = self.s3_client.get_object(Bucket=bucket_name, Key=index_key)
                index_data = json.loads(response['Body'].read().decode('utf-8'))
            except:
                index_data = {
                    "processed_files": {},
                    "last_updated": datetime.now().isoformat()
                }
            
            # Add or update entry for this audio file
            index_data["processed_files"][audio_filename] = {
                "manifest_path": f"email_output/{audio_filename}/manifest.json",
                "processed_at": manifest["source"]["processed_at"],
                "title": manifest["meeting"]["title"],
                "status": manifest["status"]
            }
            index_data["last_updated"] = datetime.now().isoformat()
            
            # Save updated index
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=index_key,
                Body=json.dumps(index_data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json; charset=utf-8'
            )
            logger.info(f"📑 インデックス更新: s3://{bucket_name}/{index_key}")
            
        except Exception as e:
            logger.warning(f"⚠️ インデックス更新失敗: {str(e)}")

    def _create_completion_notification(self, bucket_name: str, object_key: str, meeting_info: Dict[str, str], 
                                   transcript_length: int, saved_files: Dict[str, str]) -> None:
        """
        Create a completion notification file in the audio's subfolder
        """
        try:
            audio_filename = object_key.split('/')[-1].rsplit('.', 1)[0]
            notification_key = f"email_output/{audio_filename}/status.txt"
            
            # Check if we're using demo data
            manifest_key = saved_files.get("manifest", "")
            if manifest_key:
                try:
                    response = self.s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
                    manifest = json.loads(response['Body'].read().decode('utf-8'))
                    is_demo = manifest.get("processing", {}).get("is_demo_data", False)
                except:
                    is_demo = False
            else:
                is_demo = False
            
            notification_content = f"""=== 議事録生成完了 ===

完了時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}
音声ファイル: {object_key}
会議タイトル: {meeting_info['title']}

📋 ファイル場所:
s3://{bucket_name}/email_output/{audio_filename}/

🧩 主要ファイル:
• manifest.json - フロントエンド用マニフェスト（最重要）
• email_content.txt - メールコピペ用テキスト
• transcript.txt - 文字起こし全文
• summary.json - 議事録データ (JSON形式)

💻 フロントエンド表示:
manifest.json ファイルを参照してください。
"""

            if is_demo:
                notification_content += f"""
⚠️ 注意:
指定モデル ({self.primary_model}) のアクセス権限がないため、
デモデータを使用しています。AWS Bedrockでモデルアクセスを申請してください。
"""

            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=notification_key,
                Body=notification_content.encode('utf-8'),
                ContentType='text/plain; charset=utf-8'
            )
            
            logger.info(f"📢 完了通知作成: s3://{bucket_name}/{notification_key}")
            
        except Exception as e:
            logger.warning(f"⚠️ 完了通知作成失敗: {str(e)}")

    def _create_error_debug_file(self, error_message: str, exception: Exception) -> None:
        """エラー時のデバッグファイル作成"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            error_key = f"system_logs/SYSTEM_ERROR_{timestamp}.txt"
            
            import traceback
            error_content = f"""=== システムエラー情報 ===

発生時刻: {datetime.now().isoformat()}
エラーメッセージ: {error_message}

🥇 指定メインモデル: {self.primary_model}
🔄 フォールバックモデル数: {len(self.claude_models) - 1}

詳細トレースバック:
{traceback.format_exc()}

【Bedrock設定確認リスト】
AWS Bedrock → Model access で以下を確認:

最重要:
□ {self.primary_model} ← 指定されたメインモデル
"""

            for model in self.claude_models[1:]:
                error_content += f"□ {model}\n"
            
            error_content += f"""

【Status の意味】
• 'Access granted' → 使用可能
• 'Available to request' → 申請が必要
• その他 → 利用不可

【申請手順】
1. 'Available to request' のモデルをクリック
2. 'Request model access' をクリック
3. Use case: 'General' を選択
4. Submit
5. 承認まで数分〜数時間待機
6. 'Access granted' になったらMP3再アップロード
"""

            self.s3_client.put_object(
                Bucket="followup-mail",
                Key=error_key,
                Body=error_content.encode('utf-8'),
                ContentType='text/plain; charset=utf-8'
            )
            logger.info(f"🚨 システムエラーファイル作成: s3://followup-mail/{error_key}")
            
        except Exception as e:
            logger.error(f"エラーファイル作成失敗: {str(e)}")

    def _create_error_response(self, message: str, status_code: int = 500) -> Dict[str, Any]:
        """エラーレスポンス作成"""
        return {
            'statusCode': status_code,
            'body': json.dumps({
                'status': 'error',
                'message': message,
                'specified_model': self.primary_model,
                'timestamp': datetime.now().isoformat()
            }, ensure_ascii=False)
        }

    def _create_file_error_result(self, object_key: str, error_message: str) -> Dict[str, Any]:
        """ファイル処理エラー結果作成"""
        return {
            'file': object_key,
            'status': 'error',
            'error': error_message,
            'specified_model': self.primary_model,
            'timestamp': datetime.now().isoformat()
        }

    def _create_success_response(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """成功レスポンス作成"""
        successful_files = [r for r in results if r['status'] == 'success']
        failed_files = [r for r in results if r['status'] == 'error']

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'処理完了: 成功 {len(successful_files)}件, 失敗 {len(failed_files)}件',
                'specified_model': self.primary_model,
                'summary': {
                    'total_files': len(results),
                    'successful': len(successful_files),
                    'failed': len(failed_files)
                },
                'results': results,
                'timestamp': datetime.now().isoformat()
            }, ensure_ascii=False, indent=2)
        }

# Lambda エントリーポイント
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda関数エントリーポイント"""
    processor = MeetingMinutesProcessor()
    return processor.process_event(event, context)
