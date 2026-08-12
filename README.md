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

Jika Anda ingin mencoba menjalankan pipeline ini di mesin lokal, silakan ikuti langkah-langkah berikut:

### Requirements
- **Docker & Docker Compose** (untuk menjalankan Kafka broker)
- **Python 3.12+** (disarankan menggunakan *virtual environment*)

### Setup & Execution

1. **Start Kafka Cluster:**
   Jalankan Kafka broker menggunakan Docker Compose.
   ```bash
   docker-compose up -d
   ```
   *Tunggu sekitar 10 detik agar broker sepenuhnya siap menerima koneksi.*

2. **Install Dependencies:**
   Install library Python yang dibutuhkan (`kafka-python-ng` dan `Faker`).
   ```bash
   pip install -r requirements.txt
   ```

3. **Run Producer (Terminal 1):**
   ```bash
   python producer.py
   ```
   Producer akan mulai melakukan *generate* data transaksi dummy (format JSON) menggunakan Faker, dan mengirimkannya ke Kafka setiap 5 detik. Biarkan proses ini berjalan.

4. **Run Consumer Group (Terminal 2 & 3):**
   Buka dua terminal baru, lalu jalankan perintah berikut di **masing-masing terminal**:
   ```bash
   python consumer.py
   ```
   Anda akan melihat kedua consumer melakukan proses *load balancing* berkat fitur **Consumer Group**. Satu consumer akan memproses data dari partisi 0, sementara yang lainnya memproses partisi 1.

5. **Verify Data Sink:**
   Pipeline ini dikonfigurasi untuk menyimpan hasil agregasi secara otomatis ke dalam database SQLite lokal bernama `events_sink.db`. Buka file tersebut menggunakan tools seperti DBeaver atau SQLite Viewer untuk melihat ringkasan data (total events & total amount per user).
