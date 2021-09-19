# sparkによるIoTデータ処理の学習

## 以下仕様でIoTデータの収集からDBへの保存、機械学習のモデリングまでをコード化し、spark ( pyspark) による分散処理を学ぶ
![spark_machine_Learning](https://user-images.githubusercontent.com/61402011/132344753-9a526de5-c2f6-4fac-a23b-1be21e99ec0d.png)
* 学習書籍：『アプリケーションエンジニアのための Apache spark入門』（秀和システム）

## 各バージョン
| アプリ(言語) | バージョン |
| ---- | ---- |
| python | 3.8.10 |
| Fluentd (td-agent) | 4.2.0 (1.13.3) |
| kafka | 2.8.0 |
| spark | 3.1.1 |

## 各ソースコード内容
* kafka投入fluentdの設定ファイル
  * td-agent.conf
* kafka出力結果json出力コード
  * kafka_to_json.py
* parquet形式で保存
  * kafka_to_parquet.py
* kassandraへの保存
  * kafka_to_cassandra.py
* タンブリングウィンドウ平均値算出後、出力
  * culc_Tumbling_Windows_avg.py
* スライディングウィンドウ平均値算出後、出力
  * culc_Sliding_Windows_avg.py.py
* 出力先を複数フォルダに仕分け
  * kafka_classify_data_to_each_directory.py
* PCAによる特徴量数決定.ipynb
  * sparkのPCAを用いて特徴量数を決定
* 分類モデル(決定木・ロジスティクス回帰)の適用.ipynb
  * IoTデータに対して、sparkで分類モデルを作成
* 回帰モデル.ipynb
  * IoTデータに対して、sparkで回帰モデルを作成
