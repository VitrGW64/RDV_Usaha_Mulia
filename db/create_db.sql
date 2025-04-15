-- Pembuatan database
CREATE DATABASE usaha_mulia;
USE usaha_mulia;

-- 1. Tabel kota
CREATE TABLE kota (
  kota_id INT NOT NULL,
  kota_nama VARCHAR(50),
  PRIMARY KEY (kota_id)
);

-- 2. Tabel pemilik
CREATE TABLE pemilik (
  pemilik_id INT,
  pemilik_nama VARCHAR(50),
  pemilik_email VARCHAR(100),
  PRIMARY KEY (pemilik_id)
);

-- 3. Tabel gudang
CREATE TABLE gudang (
  gudang_id INT NOT NULL,
  kota_id INT,
  gudang_kapasitas INT,
  PRIMARY KEY (gudang_id),
  FOREIGN KEY (kota_id) REFERENCES kota(kota_id)
);

-- 4. Tabel minimart
CREATE TABLE minimart (
  minimart_id INT,
  kota_id INT,
  pemilik_id INT,
  gudang_id INT,
  minimart_nama VARCHAR(255),
  minimart_alamat VARCHAR(255),
  PRIMARY KEY (minimart_id),
  FOREIGN KEY (kota_id) REFERENCES kota(kota_id),
  FOREIGN KEY (pemilik_id) REFERENCES pemilik(pemilik_id),
  FOREIGN KEY (gudang_id) REFERENCES gudang(gudang_id)
);

-- 5. Tabel pegawai
CREATE TABLE pegawai (
  pegawai_id INT,
  minimart_id INT,
  pegawai_nama VARCHAR(255),
  pegawai_jabatan VARCHAR(50),
  PRIMARY KEY (pegawai_id),
  FOREIGN KEY (minimart_id) REFERENCES minimart(minimart_id)
);

-- 6. Tabel barang
CREATE TABLE barang (
  barang_id INT,
  barang_nama VARCHAR(255),
  barang_harga_beli INT,
  barang_harga_jual INT,
  barang_stok INT,
  PRIMARY KEY (barang_id)
);

-- 7. Tabel inventory
CREATE TABLE inventory (
  inventory_id INT AUTO_INCREMENT PRIMARY KEY,
  barang_id INT,
  gudang_id INT,
  inventory_stok INT,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (barang_id) REFERENCES barang(barang_id),
  FOREIGN KEY (gudang_id) REFERENCES gudang(gudang_id)
);

-- 8. Tabel transaksi
CREATE TABLE transaksi (
  transaksi_id INT,
  minimart_id INT,
  pegawai_id INT,
  tanggal_waktu TIMESTAMP,
  transaksi_total INT,
  transaksi_pembayaran INT,
  transaksi_kembalian INT,
  PRIMARY KEY (transaksi_id),
  FOREIGN KEY (minimart_id) REFERENCES minimart(minimart_id),
  FOREIGN KEY (pegawai_id) REFERENCES pegawai(pegawai_id)
);

-- 9. Tabel isi_transaksi
CREATE TABLE isi_transaksi (
  transaksi_id INT,
  barang_id INT,
  isi_transaksi_jumlah INT,
  harga_satuan INT,
  PRIMARY KEY (transaksi_id, barang_id),
  FOREIGN KEY (transaksi_id) REFERENCES transaksi(transaksi_id),
  FOREIGN KEY (barang_id) REFERENCES barang(barang_id)
);
