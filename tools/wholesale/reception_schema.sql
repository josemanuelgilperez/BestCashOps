CREATE TABLE IF NOT EXISTS wholesale_clients (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(160) NOT NULL,
  token CHAR(64) NOT NULL,
  status ENUM('active', 'disabled') NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_wholesale_clients_token (token),
  KEY idx_wholesale_clients_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS client_pallets (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  client_id BIGINT UNSIGNED NOT NULL,
  box_code VARCHAR(16) NOT NULL,
  status ENUM('active', 'closed', 'cancelled') NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_client_pallets_client_box (client_id, box_code),
  KEY idx_client_pallets_box_code (box_code),
  KEY idx_client_pallets_status (status),
  CONSTRAINT fk_client_pallets_client
    FOREIGN KEY (client_id) REFERENCES wholesale_clients (id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS pallet_reception_units (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  client_pallet_id BIGINT UNSIGNED NOT NULL,
  box_code VARCHAR(16) NOT NULL,
  asin VARCHAR(32) NOT NULL,
  unit_index INT UNSIGNED NOT NULL,
  unit_total INT UNSIGNED NOT NULL,
  status ENUM('pending', 'received', 'missing', 'damaged') NOT NULL DEFAULT 'pending',
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_reception_units_assignment_unit (client_pallet_id, asin, unit_index),
  KEY idx_reception_units_box_code (box_code),
  KEY idx_reception_units_status (status),
  CONSTRAINT fk_reception_units_client_pallet
    FOREIGN KEY (client_pallet_id) REFERENCES client_pallets (id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
