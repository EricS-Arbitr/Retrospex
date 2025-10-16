-- Connect to MySQL
-- mysql -h 11.11.10.103 -u learner -p

-- Create database
CREATE DATABASE IF NOT EXISTS hunt_results;
USE hunt_results;

-- Hunt execution tracking
CREATE TABLE hunt_executions (
    execution_id INT AUTO_INCREMENT PRIMARY KEY,
    hunt_name VARCHAR(255) NOT NULL,
    hunt_version VARCHAR(50),
    execution_start DATETIME NOT NULL,
    execution_end DATETIME,
    status ENUM('running', 'completed', 'failed') DEFAULT 'running',
    records_analyzed BIGINT,
    findings_count INT DEFAULT 0,
    date_range_start DATE,
    date_range_end DATE,
    parameters JSON,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_hunt_name (hunt_name),
    INDEX idx_execution_start (execution_start),
    INDEX idx_status (status)
);

-- Hunt findings
CREATE TABLE hunt_findings (
    finding_id INT AUTO_INCREMENT PRIMARY KEY,
    execution_id INT NOT NULL,
    hunt_name VARCHAR(255) NOT NULL,
    severity ENUM('critical', 'high', 'medium', 'low', 'info') DEFAULT 'medium',
    finding_type VARCHAR(100),
    source_ip VARCHAR(45),
    destination_ip VARCHAR(45),
    source_hostname VARCHAR(255),
    destination_hostname VARCHAR(255),
    username VARCHAR(255),
    process_name VARCHAR(255),
    command_line TEXT,
    timestamp DATETIME,
    confidence_score DECIMAL(5,2),
    description TEXT,
    raw_data JSON,
    analyst_notes TEXT,
    status ENUM('new', 'investigating', 'confirmed', 'false_positive', 'resolved') DEFAULT 'new',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES hunt_executions(execution_id),
    INDEX idx_hunt_name (hunt_name),
    INDEX idx_severity (severity),
    INDEX idx_timestamp (timestamp),
    INDEX idx_status (status),
    INDEX idx_source_ip (source_ip),
    INDEX idx_destination_ip (destination_ip)
);

-- Hunt definitions (metadata about available hunts)
CREATE TABLE hunt_definitions (
    hunt_id INT AUTO_INCREMENT PRIMARY KEY,
    hunt_name VARCHAR(255) UNIQUE NOT NULL,
    hunt_version VARCHAR(50) DEFAULT '1.0',
    description TEXT,
    data_sources JSON,
    mitre_tactics JSON,
    mitre_techniques JSON,
    author VARCHAR(255),
    created_date DATE,
    last_updated DATE,
    enabled BOOLEAN DEFAULT TRUE,
    schedule_cron VARCHAR(100),
    INDEX idx_enabled (enabled)
);

-- Aggregate statistics for dashboards
CREATE TABLE hunt_statistics (
    stat_id INT AUTO_INCREMENT PRIMARY KEY,
    hunt_name VARCHAR(255) NOT NULL,
    execution_date DATE NOT NULL,
    total_findings INT DEFAULT 0,
    critical_findings INT DEFAULT 0,
    high_findings INT DEFAULT 0,
    medium_findings INT DEFAULT 0,
    low_findings INT DEFAULT 0,
    false_positives INT DEFAULT 0,
    confirmed_threats INT DEFAULT 0,
    avg_confidence_score DECIMAL(5,2),
    records_analyzed BIGINT,
    execution_time_seconds INT,
    UNIQUE KEY unique_hunt_date (hunt_name, execution_date),
    INDEX idx_execution_date (execution_date)
);

-- IOC tracking (for deduplication)
CREATE TABLE hunt_iocs (
    ioc_id INT AUTO_INCREMENT PRIMARY KEY,
    ioc_type ENUM('ip', 'domain', 'hash', 'email', 'url', 'user', 'process') NOT NULL,
    ioc_value VARCHAR(500) NOT NULL,
    first_seen DATETIME NOT NULL,
    last_seen DATETIME NOT NULL,
    times_seen INT DEFAULT 1,
    associated_hunts JSON,
    threat_level ENUM('critical', 'high', 'medium', 'low') DEFAULT 'medium',
    notes TEXT,
    UNIQUE KEY unique_ioc (ioc_type, ioc_value),
    INDEX idx_ioc_value (ioc_value),
    INDEX idx_last_seen (last_seen)
);

GRANT ALL PRIVILEGES ON hunt_results.* TO 'learner'@'%';
FLUSH PRIVILEGES;

SELECT 'Database setup complete!' AS status;