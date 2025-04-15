-- Pembuatan database staging
CREATE DATABASE IF NOT EXISTS usaha_mulia_staging;
USE usaha_mulia_staging;

-- Tabel staging untuk data transaksi
CREATE TABLE IF NOT EXISTS staging_transaksi (
    transaksi_id INT,
    minimart_id INT,
    pegawai_id INT,
    tanggal_waktu TIMESTAMP,
    transaksi_total INT,
    transaksi_pembayaran INT,
    transaksi_kembalian INT,
    PRIMARY KEY (transaksi_id)
);

-- Tabel staging untuk data isi_transaksi
CREATE TABLE IF NOT EXISTS staging_isi_transaksi (
    transaksi_id INT,
    barang_id INT,
    isi_transaksi_jumlah INT,
    harga_satuan INT,
    PRIMARY KEY (transaksi_id, barang_id)
);

-- Tambahkan tabel staging lainnya jika diperlukan
-- Contoh:
-- CREATE TABLE IF NOT EXISTS staging_barang (
--     barang_id INT,
--     barang_nama VARCHAR(255),
--     barang_harga_beli INT,
--     barang_harga_jual INT,
--     barang_stok INT,
--     PRIMARY KEY (barang_id)
-- );
