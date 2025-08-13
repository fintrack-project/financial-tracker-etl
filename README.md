# FinTrack ETL Pipeline 🚀

> **A production-ready Python ETL system that automates financial market data ingestion, processing, and distribution with real-time capabilities and enterprise-grade reliability.**

The **FinTrack ETL Pipeline** is a **sophisticated data engineering solution** built with Python that demonstrates modern ETL practices, real-time data processing, and scalable data architecture. It provides automated ingestion of financial market data from multiple sources, comprehensive data transformation, and reliable data distribution through Kafka messaging for downstream consumption.

---

## 🎯 **What This ETL Pipeline Showcases**

- **Data Engineering Excellence**: Python-based ETL with pandas, numpy, and modern data processing libraries
- **Real-time Processing**: Live market data ingestion with minimal latency
- **Data Quality**: Comprehensive validation, cleaning, and error handling
- **Scalable Architecture**: Modular job design with configurable scheduling
- **Integration Capabilities**: Kafka messaging, PostgreSQL storage, and external API integration
- **Monitoring & Observability**: Comprehensive logging, error tracking, and performance metrics
- **DevOps Ready**: Docker containerization, CI/CD integration, and environment management
- **Testing Strategy**: Unit tests, integration tests, and data validation tests

---

## 🏗️ **Technical Architecture**

### **ETL Pipeline Architecture**
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───►│   ETL Jobs      │───►│   Data Storage  │
│                 │    │                 │    │                 │
│ • Stock APIs    │    │ • Extraction    │    │ • PostgreSQL    │
│ • Crypto APIs   │    │ • Transformation│    │ • Kafka Topics  │
│ • Forex APIs    │    │ • Loading       │    │ • Data Lakes    │
│ • Market Data   │    │ • Validation    │    │ • Analytics     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐              │
         └─────────────►│   Data Quality  │◄─────────────┘
                        │   & Monitoring  │
                        └─────────────────┘
```

### **Job Processing Flow**
```
1. Data Extraction → 2. Data Validation → 3. Data Transformation → 4. Data Loading → 5. Event Publishing
     ↓                      ↓                      ↓                      ↓              ↓
  API Calls            Schema Check           Data Cleaning          Database        Kafka
  Rate Limiting        Data Quality          Aggregation            Storage         Messages
  Error Handling       Duplicate Check       Calculations           Constraints     Notifications
```

---

## 🚀 **Core Features & Capabilities**

### **📊 Data Ingestion & Sources**
- **Real-time Market Data**: Live stock prices, crypto rates, and forex data
- **Historical Data**: Comprehensive historical market data for analysis and backtesting
- **Multiple Data Sources**: Integration with major financial data providers
- **Rate Limiting**: Intelligent API rate limiting and request optimization
- **Error Recovery**: Automatic retry mechanisms and fallback strategies
- **Data Validation**: Schema validation and data quality checks

### **🔄 Data Processing & Transformation**
- **Data Cleaning**: Removal of duplicates, outliers, and invalid data
- **Data Aggregation**: Time-based aggregation and statistical calculations
- **Currency Conversion**: Multi-currency support with real-time exchange rates
- **Data Enrichment**: Additional metadata and calculated fields
- **Batch Processing**: Efficient batch processing for large datasets
- **Stream Processing**: Real-time data processing capabilities

### **💾 Data Storage & Distribution**
- **PostgreSQL Integration**: Relational database storage with optimized schemas
- **Kafka Messaging**: Event-driven data distribution and notifications
- **Data Versioning**: Historical data tracking and version management
- **Backup & Recovery**: Automated backup strategies and data recovery
- **Data Archiving**: Long-term data storage and archival policies
- **Performance Optimization**: Database indexing and query optimization

### **🔍 Monitoring & Observability**
- **Comprehensive Logging**: Structured logging with different log levels
- **Performance Metrics**: Processing time, throughput, and error rates
- **Data Quality Metrics**: Validation results and data completeness
- **Alerting System**: Automated alerts for failures and data quality issues
- **Health Checks**: System health monitoring and status reporting
- **Audit Trails**: Complete audit trail for all data processing operations

---

## 🛠️ **Technology Stack**

### **Core Python Libraries**
| **Technology** | **Version** | **Purpose** |
|----------------|-------------|-------------|
| **Python** | 3.11+ | Core programming language |
| **Pandas** | 2.2.3 | Data manipulation and analysis |
| **NumPy** | 2.2.6 | Numerical computing and arrays |
| **Requests** | 2.32.3 | HTTP client for API calls |
| **BeautifulSoup4** | 4.13.4 | Web scraping and HTML parsing |
| **LXML** | 5.4.0 | XML and HTML processing |

### **Data Storage & Integration**
| **Technology** | **Version** | **Purpose** |
|----------------|-------------|-------------|
| **psycopg** | 3.2.9 | PostgreSQL database adapter |
| **Confluent Kafka** | 2.10.0 | Message queuing and streaming |
| **SQLAlchemy** | Latest | Database ORM and connection management |
| **Alembic** | Latest | Database migration management |

### **Testing & Quality**
| **Technology** | **Version** | **Purpose** |
|----------------|-------------|-------------|
| **pytest** | 8.0.0 | Testing framework |
| **pytest-cov** | 5.0.0 | Test coverage reporting |
| **Coverage** | 7.4.0 | Code coverage analysis |
| **Mock** | Built-in | Mocking and test doubles |

### **DevOps & Deployment**
| **Technology** | **Version** | **Purpose** |
|----------------|-------------|-------------|
| **Docker** | Latest | Containerization |
| **Docker Compose** | Latest | Multi-container orchestration |
| **GitHub Actions** | Latest | CI/CD automation |
| **Environment Variables** | Built-in | Configuration management |

---

## 🔧 **Development Setup**

### **Prerequisites**
- **Python**: Version 3.11 or higher
- **PostgreSQL**: Version 17.0 or higher
- **Docker**: Version 20.10 or higher (for containerized development)
- **Git**: For version control
- **Virtual Environment**: Python virtual environment management

### **Quick Start** ⚡

```bash
# Clone the repository
git clone https://github.com/fintrack-project/financial-tracker-etl.git
cd financial-tracker-etl

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# Run ETL jobs
python -m etl.jobs.fetch_market_data.fetch_market_data
python -m etl.jobs.fetch_historical_market_data.fetch_historical_market_data
```

### **Environment Configuration**
Create a `.env` file with your configuration:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=financial_tracker
DB_USER=admin
DB_PASSWORD=secure_password_123

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_MARKET_DATA=market-data
KAFKA_TOPIC_NOTIFICATIONS=notifications

# API Configuration
API_KEY_ALPHA_VANTAGE=your_api_key_here
API_KEY_FINNHUB=your_api_key_here
API_RATE_LIMIT=100

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=etl.log
LOG_FORMAT=json
```

---

## 🐳 **Docker Development**

### **Build and Run with Docker**
```bash
# Build the Docker image
docker build -t fintrack-etl:latest .

# Run with Docker Compose (recommended)
cd ../financial-tracker-infra
docker-compose -f docker-compose.yml -f docker-compose.local.yml up -d

# Run individual container
docker run -d \
  --name fintrack-etl \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5433 \
  -e DB_NAME=financial_tracker \
  -e DB_USER=admin \
  -e DB_PASSWORD=secure_password_123 \
  fintrack-etl:latest
```

### **Docker Compose Integration**
```yaml
# docker-compose.yml
services:
  etl:
    build: ./financial-tracker-etl
    environment:
      - DB_HOST=db
      - DB_PORT=5432
      - DB_NAME=financial_tracker
      - DB_USER=admin
      - DB_PASSWORD=secure_password_123
    depends_on:
      - db
      - kafka
    volumes:
      - ./logs:/app/logs
      - ./data:/app/data
```

---

## 📊 **ETL Jobs Overview**

### **1. Market Data Fetching Jobs**

#### **Real-time Market Data (`fetch_market_data`)**
```python
# Job configuration
JOB_CONFIG = {
    'name': 'fetch_market_data',
    'schedule': '*/5 * * * *',  # Every 5 minutes
    'batch_size': 1000,
    'retry_attempts': 3,
    'timeout': 300  # 5 minutes
}

# Data sources
DATA_SOURCES = [
    'alpha_vantage',
    'finnhub',
    'yahoo_finance',
    'coinbase'
]
```

**Features:**
- **Real-time Updates**: Live market data every 5 minutes
- **Multiple Sources**: Aggregation from multiple data providers
- **Rate Limiting**: Intelligent API rate limiting
- **Error Handling**: Automatic retry and fallback mechanisms
- **Data Validation**: Schema validation and quality checks

#### **Historical Market Data (`fetch_historical_market_data`)**
```python
# Historical data configuration
HISTORICAL_CONFIG = {
    'start_date': '2020-01-01',
    'end_date': '2024-12-31',
    'symbols': ['AAPL', 'GOOGL', 'MSFT', 'BTC', 'ETH'],
    'intervals': ['1d', '1h', '15m'],
    'batch_size': 500
}
```

**Features:**
- **Gap Detection**: Identifies missing historical data
- **Incremental Updates**: Only fetches missing data
- **Batch Processing**: Efficient processing of large datasets
- **Data Integrity**: Ensures complete historical coverage

#### **Market Index Data (`fetch_market_index_data`)**
```python
# Index data configuration
INDEX_CONFIG = {
    'indices': ['^GSPC', '^DJI', '^IXIC', '^VIX'],
    'update_frequency': '1h',
    'include_volume': True,
    'include_indicators': True
}
```

**Features:**
- **Major Indices**: S&P 500, Dow Jones, NASDAQ, VIX
- **Technical Indicators**: Moving averages, RSI, MACD
- **Volume Analysis**: Trading volume and market activity
- **Volatility Metrics**: Market volatility and risk indicators

### **2. Data Processing Utilities**

#### **Data Validation (`data_validator.py`)**
```python
class DataValidator:
    def validate_market_data(self, data: pd.DataFrame) -> ValidationResult:
        """Validate market data for completeness and quality."""
        checks = [
            self._check_required_columns(data),
            self._check_data_types(data),
            self._check_value_ranges(data),
            self._check_timestamps(data),
            self._check_duplicates(data)
        ]
        return ValidationResult(checks)
    
    def _check_required_columns(self, data: pd.DataFrame) -> CheckResult:
        """Check if all required columns are present."""
        required_columns = ['symbol', 'timestamp', 'price', 'volume']
        missing_columns = set(required_columns) - set(data.columns)
        
        return CheckResult(
            name='required_columns',
            passed=len(missing_columns) == 0,
            details={'missing_columns': list(missing_columns)}
        )
```

#### **Data Transformation (`data_transformer.py`)**
```python
class DataTransformer:
    def transform_market_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform raw market data into standardized format."""
        transformed = data.copy()
        
        # Standardize column names
        transformed.columns = transformed.columns.str.lower()
        
        # Convert timestamps
        transformed['timestamp'] = pd.to_datetime(transformed['timestamp'])
        
        # Calculate additional metrics
        transformed['price_change'] = transformed['price'].diff()
        transformed['price_change_pct'] = transformed['price'].pct_change()
        
        # Add technical indicators
        transformed['sma_20'] = transformed['price'].rolling(20).mean()
        transformed['sma_50'] = transformed['price'].rolling(50).mean()
        
        return transformed
```

---

## 🧪 **Testing Strategy**

### **Testing Pyramid**
```
    🔺 E2E Tests (Few)
   🔺🔺 Integration Tests
  🔺🔺🔺 Unit Tests (Many)
```

### **Unit Testing**
```python
import pytest
from unittest.mock import Mock, patch
from etl.jobs.fetch_market_data.fetch_market_data import MarketDataFetcher

class TestMarketDataFetcher:
    
    def setup_method(self):
        self.fetcher = MarketDataFetcher()
        self.mock_api_client = Mock()
        self.fetcher.api_client = self.mock_api_client
    
    def test_fetch_stock_data_success(self):
        # Given
        symbol = 'AAPL'
        expected_data = {'symbol': 'AAPL', 'price': 150.00}
        self.mock_api_client.get_stock_price.return_value = expected_data
        
        # When
        result = self.fetcher.fetch_stock_data(symbol)
        
        # Then
        assert result == expected_data
        self.mock_api_client.get_stock_price.assert_called_once_with(symbol)
    
    def test_fetch_stock_data_api_error(self):
        # Given
        symbol = 'AAPL'
        self.mock_api_client.get_stock_price.side_effect = Exception('API Error')
        
        # When/Then
        with pytest.raises(Exception, match='API Error'):
            self.fetcher.fetch_stock_data(symbol)
```

### **Integration Testing**
```python
import pytest
from etl.utils.db_utils import DatabaseConnection
from etl.jobs.fetch_market_data.fetch_market_data import MarketDataFetcher

@pytest.mark.integration
class TestMarketDataIntegration:
    
    @pytest.fixture(autouse=True)
    def setup_database(self):
        """Set up test database."""
        self.db = DatabaseConnection(
            host='localhost',
            port=5433,
            database='financial_tracker_test',
            user='admin',
            password='secure_password_123'
        )
        self.fetcher = MarketDataFetcher()
        yield
        self.db.close()
    
    def test_end_to_end_market_data_fetch(self):
        """Test complete end-to-end market data fetching."""
        # Given
        symbol = 'AAPL'
        
        # When
        data = self.fetcher.fetch_stock_data(symbol)
        self.fetcher.save_to_database(data)
        
        # Then
        saved_data = self.db.query("SELECT * FROM market_data WHERE symbol = %s", (symbol,))
        assert len(saved_data) > 0
        assert saved_data[0]['symbol'] == symbol
```

### **Data Quality Testing**
```python
import pytest
import pandas as pd
from etl.utils.data_validator import DataValidator

class TestDataQuality:
    
    def setup_method(self):
        self.validator = DataValidator()
    
    def test_validate_market_data_completeness(self):
        """Test market data completeness validation."""
        # Given
        data = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT'],
            'timestamp': ['2024-01-01', '2024-01-01', '2024-01-01'],
            'price': [150.00, 2800.00, 350.00],
            'volume': [1000000, 500000, 800000]
        })
        
        # When
        result = self.validator.validate_market_data(data)
        
        # Then
        assert result.is_valid
        assert result.completeness_score == 1.0
    
    def test_validate_market_data_quality(self):
        """Test market data quality validation."""
        # Given
        data = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT'],
            'timestamp': ['2024-01-01', '2024-01-01', '2024-01-01'],
            'price': [150.00, -2800.00, 350.00],  # Negative price
            'volume': [1000000, 500000, 800000]
        })
        
        # When
        result = self.validator.validate_market_data(data)
        
        # Then
        assert not result.is_valid
        assert result.quality_score < 1.0
```

---

## 📈 **Performance & Monitoring**

### **Performance Metrics**
```python
# Performance monitoring
import time
from functools import wraps

def performance_monitor(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss
        
        try:
            result = func(*args, **kwargs)
            success = True
        except Exception as e:
            success = False
            raise e
        finally:
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss
            
            execution_time = end_time - start_time
            memory_usage = end_memory - start_memory
            
            # Log performance metrics
            logger.info(f"Function: {func.__name__}, "
                       f"Execution Time: {execution_time:.2f}s, "
                       f"Memory Usage: {memory_usage / 1024 / 1024:.2f}MB, "
                       f"Success: {success}")
        
        return result
    return wrapper
```

### **Data Quality Metrics**
```python
class DataQualityMonitor:
    def calculate_quality_metrics(self, data: pd.DataFrame) -> QualityMetrics:
        """Calculate comprehensive data quality metrics."""
        return QualityMetrics(
            completeness=self._calculate_completeness(data),
            accuracy=self._calculate_accuracy(data),
            consistency=self._calculate_consistency(data),
            timeliness=self._calculate_timeliness(data),
            validity=self._calculate_validity(data)
        )
    
    def _calculate_completeness(self, data: pd.DataFrame) -> float:
        """Calculate data completeness score."""
        total_cells = data.size
        non_null_cells = data.notna().sum().sum()
        return non_null_cells / total_cells if total_cells > 0 else 0.0
```

---

## 🔐 **Security & Data Protection**

### **API Security**
- **API Key Management**: Secure storage and rotation of API keys
- **Rate Limiting**: Intelligent rate limiting to prevent API abuse
- **Request Signing**: Secure request signing for authenticated APIs
- **Error Handling**: Secure error messages without information leakage

### **Data Security**
- **Data Encryption**: Encryption at rest and in transit
- **Access Control**: Role-based access control for data operations
- **Audit Logging**: Complete audit trail for all data operations
- **Data Masking**: Sensitive data masking in logs and outputs

---

## 🚀 **Deployment & Production**

### **Production Configuration**
```bash
# Production environment variables
export ENVIRONMENT=production
export LOG_LEVEL=WARNING
export LOG_FORMAT=json
export METRICS_ENABLED=true
export ALERTING_ENABLED=true
export BACKUP_ENABLED=true
export MONITORING_ENABLED=true
```

### **Scheduling & Orchestration**
```python
# Cron job configuration
CRON_SCHEDULES = {
    'market_data': '*/5 * * * *',      # Every 5 minutes
    'historical_data': '0 2 * * *',    # Daily at 2 AM
    'index_data': '0 * * * *',         # Every hour
    'data_cleanup': '0 3 * * 0',       # Weekly cleanup
    'backup': '0 1 * * *'              # Daily backup
}
```

### **Health Checks**
```python
class HealthChecker:
    def check_system_health(self) -> HealthStatus:
        """Check overall system health."""
        checks = {
            'database': self._check_database_connection(),
            'kafka': self._check_kafka_connection(),
            'api_endpoints': self._check_api_endpoints(),
            'disk_space': self._check_disk_space(),
            'memory_usage': self._check_memory_usage()
        }
        
        overall_health = all(check['healthy'] for check in checks.values())
        
        return HealthStatus(
            healthy=overall_health,
            checks=checks,
            timestamp=datetime.utcnow()
        )
```

---

## 🔮 **Future Enhancements**

### **Planned Features**
- **Machine Learning Integration**: ML-powered data quality and anomaly detection
- **Real-time Streaming**: Apache Kafka Streams for real-time processing
- **Data Lake Integration**: Integration with data lakes for big data analytics
- **Advanced Analytics**: Statistical analysis and predictive modeling
- **Multi-cloud Support**: Support for multiple cloud providers

### **Technical Improvements**
- **Apache Airflow**: Advanced workflow orchestration and scheduling
- **Apache Spark**: Big data processing for large datasets
- **GraphQL APIs**: Advanced data querying and filtering
- **Microservices**: Service decomposition for better scalability
- **Event Sourcing**: Event-driven architecture for data lineage

---

## 🤝 **Contributing to ETL**

### **Development Guidelines**
- **Code Style**: Follow PEP 8 and use Black for formatting
- **Testing**: Comprehensive test coverage for all new features
- **Documentation**: Clear docstrings and README updates
- **Data Quality**: Data validation and quality checks for all jobs
- **Performance**: Consider performance implications of all changes

### **Pull Request Process**
1. **Fork** the repository
2. **Create** a feature branch
3. **Implement** your changes with tests
4. **Update** documentation and README
5. **Submit** a pull request
6. **Code Review** and iteration

---

## 📚 **Additional Resources**

### **Documentation**
- [Python Documentation](https://docs.python.org/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [NumPy Documentation](https://numpy.org/doc/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

### **Learning Resources**
- **Data Engineering**: Modern ETL practices and patterns
- **Python Best Practices**: Code quality and performance optimization
- **Database Design**: Schema design and optimization
- **Data Quality**: Validation, monitoring, and improvement strategies

---

## 📞 **Support & Community**

- **GitHub Issues**: Bug reports and feature requests
- **Discussions**: Community forum for questions
- **Documentation**: Comprehensive guides and examples
- **Contributing**: Guidelines for contributors

---

## 🏆 **Why This ETL Pipeline Stands Out**

This ETL pipeline demonstrates **enterprise-grade data engineering** with:

- **Modern Python**: Latest Python features and best practices
- **Data Quality**: Comprehensive validation and monitoring
- **Performance**: Optimized processing and efficient data handling
- **Scalability**: Modular design and horizontal scaling
- **Reliability**: Robust error handling and recovery mechanisms
- **DevOps Ready**: Containerization and CI/CD integration

**FinTrack ETL Pipeline** represents a **production-ready data processing system** that showcases the ability to build robust, scalable, and maintainable ETL solutions while following industry best practices and modern data engineering standards.

---

*Built with ❤️ using Python 3.11, modern data libraries, and enterprise-grade technologies*