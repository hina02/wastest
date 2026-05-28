
## Agent pipeline
// 概念的なパイプラインのイメージ
Fetch (API/HTML) 
  => パース済みテキスト (Struct)
  => tokio MPSC Channel (送信)
  ... (tokio並列ワーカー) ...
  => tokio MPSC Channel (受信)
  => rig-core Agent (プロンプト適用 & LLM呼び出し)
  => 構造化データ (Statement/Keyword Structへデシリアライズ)
  => DuckDB/SQLiteへ保存