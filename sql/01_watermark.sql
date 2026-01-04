-- Watermark table to track incremental processing
CREATE TABLE meta.watermark (
  dataset_name      VARCHAR(100) NOT NULL PRIMARY KEY,
  last_processed_ts DATETIME2     NOT NULL
);

-- Initialize
INSERT INTO meta.watermark(dataset_name, last_processed_ts)
VALUES
('transactions', '1900-01-01'),
('customers',    '1900-01-01'),
('products',     '1900-01-01');
