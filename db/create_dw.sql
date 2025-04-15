-- 1. Dimension Table: Kota
CREATE TABLE dim_kota (
    kota_id INT PRIMARY KEY,
    kota_nama VARCHAR(50)
);

-- 2. Dimension Table: Gudang
CREATE TABLE dim_gudang (
    gudang_id INT PRIMARY KEY,
    kota_id INT,
    gudang_kapasitas INT,
    INDEX (kota_id),
    FOREIGN KEY (kota_id) REFERENCES dim_kota(kota_id)
);

-- 3. Dimension Table: Minimart
CREATE TABLE dim_minimart (
    minimart_id INT PRIMARY KEY,
    minimart_nama VARCHAR(255),
    kota_id INT,
    gudang_id INT,
    minimart_alamat VARCHAR(255),
    INDEX (kota_id),
    INDEX (gudang_id),
    FOREIGN KEY (kota_id) REFERENCES dim_kota(kota_id),
    FOREIGN KEY (gudang_id) REFERENCES dim_gudang(gudang_id)
);

-- 4. Dimension Table: Cashier
CREATE TABLE dim_cashier (
    cashier_id INT PRIMARY KEY,
    cashier_nama VARCHAR(255),
    minimart_id INT,
    INDEX (minimart_id),
    FOREIGN KEY (minimart_id) REFERENCES dim_minimart(minimart_id)
);

-- 5. Dimension Table: Barang
CREATE TABLE dim_barang (
    barang_id INT PRIMARY KEY,
    barang_nama VARCHAR(255),
    barang_kategori VARCHAR(50)
);

-- 6. Dimension Table: Waktu
CREATE TABLE dim_waktu (
    waktu_id INT PRIMARY KEY,
    tanggal DATE,
    jam INT,
    hari VARCHAR(20),
    minggu INT,
    bulan INT,
    tahun INT
);

-- 7. Fact Table: Sales
CREATE TABLE fact_sales (
    sales_id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id INT,  -- Optional link to OLTP if needed
    minimart_id INT,
    cashier_id INT,
    barang_id INT,
    waktu_id INT,
    total_amount DECIMAL(10, 2),
    payment_amount DECIMAL(10, 2),
    change_amount DECIMAL(10, 2),
    profit DECIMAL(10, 2),
    quantity_sold INT,
    sales_datetime DATETIME,
    INDEX (minimart_id),
    INDEX (cashier_id),
    INDEX (barang_id),
    INDEX (waktu_id),
    FOREIGN KEY (minimart_id) REFERENCES dim_minimart(minimart_id),
    FOREIGN KEY (cashier_id) REFERENCES dim_cashier(cashier_id),
    FOREIGN KEY (barang_id) REFERENCES dim_barang(barang_id),
    FOREIGN KEY (waktu_id) REFERENCES dim_waktu(waktu_id)
);
