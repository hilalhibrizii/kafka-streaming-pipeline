# Report Assignment Kafka

## 1. Bagaimana Kafka Menentukan Partition Berdasarkan Key

Kafka menggunakan mekanisme hashing untuk menentukan partition tujuan dari setiap event yang dikirim oleh producer.

Prosesnya:
- Setiap event yang dikirim memiliki key (contoh: "user_1", "user_2", "user_3").
- Kafka menjalankan fungsi hash (murmur2) terhadap key tersebut, lalu hasilnya di-modulo dengan jumlah partition yang tersedia.
- Rumusnya: `partition = hash(key) % jumlah_partition`
- Karena kita menggunakan 2 partition, maka hasilnya selalu 0 atau 1.
- Key yang sama akan selalu menghasilkan hash yang sama, sehingga event dengan key "user_1" pasti selalu masuk ke partition yang sama. Ini menjaga urutan data per-user tetap konsisten.

Kalau producer tidak menyertakan key (null), Kafka akan mendistribusikan event secara round-robin ke semua partition tanpa jaminan urutan.

## 2. Observasi Consumer Group

Konfigurasi yang digunakan:
- Topic: `events_topic` dengan 2 partition
- Consumer group: `assignment_group`
- Jumlah consumer: 2 (dijalankan di terminal terpisah)

Hasil pengamatan:
- Saat consumer pertama dijalankan sendirian, dia mendapat assignment untuk kedua partition (partition 0 dan partition 1). Semua event masuk ke consumer ini.
- Ketika consumer kedua dijalankan, Kafka melakukan proses rebalancing. Partition dibagi rata: consumer 1 dapat partition 0, consumer 2 dapat partition 1 (atau sebaliknya).
- Setelah rebalance selesai, masing-masing consumer hanya memproses event dari partition yang di-assign ke dia. Misalnya consumer 1 hanya memproses event dari "user_1" dan "user_3", sedangkan consumer 2 hanya memproses "user_2".
- Tidak ada event yang diproses dua kali (duplikasi), karena satu partition hanya bisa dibaca oleh satu consumer dalam group yang sama.
- Ketika salah satu consumer dimatikan, Kafka melakukan rebalance lagi dan partition yang ditinggalkan akan diambil alih oleh consumer yang masih aktif.

Kesimpulan: consumer group memungkinkan load balancing otomatis antar consumer, dimana setiap consumer bertanggung jawab atas partition tertentu saja.

## 3. How to Run

### Requirements
- **Docker & Docker Compose** (for Kafka Broker)
- **Python 3.12+** (virtual environment recommended)

### Setup & Execution

1. **Start Kafka Cluster:**
   ```bash
   docker-compose up -d
   ```
   *Wait ~10 seconds for the broker to initialize.*

2. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run Producer (Terminal 1):**
   ```bash
   python producer.py
   ```
   *Generates dummy JSON transaction data via Faker and streams to Kafka every 5 seconds.*

4. **Run Consumer Group (Terminal 2 & 3):**
   Open two new terminals and run the following command in **each terminal**:
   ```bash
   python consumer.py
   ```
   *The two consumers will automatically load balance the partitions within the `assignment_group`.*

5. **Data Sink Verification:**
   The consumer pipeline automatically aggregates and writes the output to a local SQLite database (`events_sink.db`). Use DBeaver or any SQLite viewer to verify the `user_stats` table for event counts and total amounts.
